#include <linux/module.h>
#include <linux/slab.h>
#include <linux/delay.h>
#include <linux/workqueue.h>
#include <linux/rculist.h>
#include <linux/cpu.h>
#include <linux/random.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("roman.penyaev@profitbricks.com");

struct conn {
	struct list_head entry;
	bool deleted;
};

struct stats {
	u64 io_observed_nulls_num;
	u64 io_observed_valid_num;
	u64 io_doubl_sync_rcu_num;
	u64 io_found_dang_ptr_num;

	u64 rm_observed_nulls_num;
	u64 rm_observed_valid_num;

	u64 adds_num;
};

static DEFINE_MUTEX(s_conns_mutex);
static DEFINE_MUTEX(s_rm_mutex);
static DEFINE_MUTEX(s_mutex);
static /* __rcu */ LIST_HEAD(s_conns_list);
static struct conn  __rcu * __percpu *s_pcpu_conn;
static struct stats  __percpu *s_pcpu_stats;
static struct workqueue_struct *s_wq;
static bool stop_please;
static u64 observed_null_jiffies;

#define THROTTLE_ADD_SECS 10

static struct work_struct s_io_works[NR_CPUS];
static struct delayed_work s_add_works[NR_CPUS];
static struct delayed_work s_rm_works[NR_CPUS];

static void rcu_rr_start(void);
static void rcu_rr_stop(void);
static inline void remove_conn_from_arr(struct conn *conn);

static int get_stats(char *buffer, const struct kernel_param *kp)
{
	struct stats stats, *s;
	char throttle_buf[32];
	u64 throttle_jiffies;
	int cpu;

	memset(&stats, 0, sizeof(stats));

	for_each_possible_cpu(cpu) {
		s = per_cpu_ptr(s_pcpu_stats, cpu);
		stats.io_observed_valid_num += s->io_observed_valid_num;
		stats.io_observed_nulls_num += s->io_observed_nulls_num;
		stats.io_doubl_sync_rcu_num += s->io_doubl_sync_rcu_num;
		stats.io_found_dang_ptr_num += s->io_found_dang_ptr_num;
		stats.rm_observed_valid_num += s->rm_observed_valid_num;
		stats.rm_observed_nulls_num += s->rm_observed_nulls_num;
		stats.adds_num += s->adds_num;
	}

	throttle_jiffies = observed_null_jiffies + HZ * THROTTLE_ADD_SECS;
	if (time_is_before_jiffies64(throttle_jiffies))
		snprintf(throttle_buf, sizeof(throttle_buf),
			 "NOW");
	else
		snprintf(throttle_buf, sizeof(throttle_buf), "%u secs",
			 jiffies_to_msecs(throttle_jiffies - get_jiffies_64())
			 /1000);

	return snprintf(buffer, 4096,
			"Throttle add in:    %s\n\n"
			"IO observed valid:  %llu\n"
			"IO observed nulls:  %llu\n"
			"IO double sync rcu: %llu\n"
			"IO found dangl ptr: %llu\n\n"
			"RM observed valid:  %llu\n"
			"RM observed nulls:  %llu\n\n"
			"Adds: %llu\n",
			throttle_buf,
			stats.io_observed_valid_num,
			stats.io_observed_nulls_num,
			stats.io_doubl_sync_rcu_num,
			stats.io_found_dang_ptr_num,
			stats.rm_observed_valid_num,
			stats.rm_observed_nulls_num,
			stats.adds_num);
}

static const struct kernel_param_ops stats_ops = {
	.get = get_stats,
};
module_param_cb(stats, &stats_ops, NULL, 0444);

static int set_start(const char *val, const struct kernel_param *kp)
{
	rcu_rr_start();

	return 0;
}

static const struct kernel_param_ops start_ops = {
	.set = set_start,
};
module_param_cb(start, &start_ops, NULL, 0200);

static int set_stop(const char *val, const struct kernel_param *kp)
{
	rcu_rr_stop();

	return 0;
}

static const struct kernel_param_ops stop_ops = {
	.set = set_stop,
};
module_param_cb(stop, &stop_ops, NULL, 0200);

static void free_conn(struct conn *conn)
{
	conn->deleted = true;
	smp_wmb();
	kfree(conn);
}

static void free_conns_list(void)
{
	struct conn *conn, *tmp;
	int cpu;

	/*
	 * Obviously here at this point s_wq should be drained!
	 */
	WARN_ON(!stop_please);

	list_for_each_entry_safe(conn, tmp, &s_conns_list, entry) {
		list_del(&conn->entry);
		free_conn(conn);
	}
	/* Reset all dangling pointers */
	for_each_possible_cpu(cpu) {
		memset(per_cpu_ptr(s_pcpu_conn, cpu), 0,
		       sizeof(*s_pcpu_conn));
	}
}

/**
 * list_next_or_null_rr_rcu - get next list element in round-robin fashion.
 * @head:	the head for the list.
 * @ptr:        the list head to take the next element from.
 * @type:       the type of the struct this is embedded in.
 * @memb:       the name of the list_head within the struct.
 *
 * Next element returned in round-robin fashion, i.e. head will be skipped,
 * but if list is observed as empty, NULL will be returned.
 *
 * This primitive may safely run concurrently with the _rcu list-mutation
 * primitives such as list_add_rcu() as long as it's guarded by rcu_read_lock().
 */
#define list_next_or_null_rr_rcu(head, ptr, type, memb) \
({ \
	list_next_or_null_rcu(head, ptr, type, memb) ?: \
		list_next_or_null_rcu(head, READ_ONCE((ptr)->next), type, memb); \
})

/**
 * get_next_conn_rr() - Returns conn in round-robin fashion.
 *
 * Locks:
 *    rcu_read_lock() must be hold.
 */
static inline struct conn *get_next_conn_rr(void)
{
	struct conn __rcu **ppcpu_conn;
	struct conn *conn;

	/*
	 * Here we use two RCU objects: @conn_list and @pcpu_conn
	 * pointer.  See remove_conn_from_arr() for details how that
	 * is handled.
	 */

	ppcpu_conn = this_cpu_ptr(s_pcpu_conn);
	conn = rcu_dereference(*ppcpu_conn);
	if (unlikely(!conn))
		conn = list_first_or_null_rcu(&s_conns_list,
					      typeof(*conn), entry);
	else
		conn = list_next_or_null_rr_rcu(&s_conns_list,
						&conn->entry,
						typeof(*conn),
						entry);
	rcu_assign_pointer(*ppcpu_conn, conn);

	return conn;
}

static inline void add_conn_to_arr(struct conn *conn)
{
	mutex_lock(&s_conns_mutex);
	list_add_tail_rcu(&conn->entry, &s_conns_list);
	mutex_unlock(&s_conns_mutex);
}

static inline bool xchg_conns(struct conn __rcu **rcu_ppcpu_conn,
			      struct conn *conn,
			      struct conn *next)
{
	struct conn **ppcpu_conn;

	/* Call cmpxchg() without sparse warnings */
	ppcpu_conn = (typeof(ppcpu_conn))rcu_ppcpu_conn;
	return (conn == cmpxchg(ppcpu_conn, conn, next));
}

static inline void remove_conn_from_arr(struct conn *conn)
{
	bool wait_for_grace = false;
	struct conn *next;
	int cpu;

	mutex_lock(&s_conns_mutex);
	list_del_rcu(&conn->entry);

	/* Make sure everybody observes conn removal. */
	synchronize_rcu();

	/*
	 * At this point nobody sees @conn in the list, but still we have
	 * dangling pointer @pcpu_conn which _can_ point to @conn.  Since
	 * nobody can observe @conn in the list, we guarantee that IO path
	 * will not assign @conn to @pcpu_conn, i.e. @pcpu_conn can be equal
	 * to @conn, but can never again become @conn.
	 */

	/*
	 * Get @next connection from current @conn which is going to be
	 * removed.  If @conn is the last element, then @next is NULL.
	 */
	next = list_next_or_null_rr_rcu(&s_conns_list, &conn->entry,
					typeof(*next), entry);

	/*
	 * @pcpu conns can still point to the conn which is going to be
	 * removed, so change the pointer manually.
	 */
	for_each_possible_cpu(cpu) {
		struct conn __rcu **ppcpu_conn;

		ppcpu_conn = per_cpu_ptr(s_pcpu_conn, cpu);
		if (rcu_dereference(*ppcpu_conn) != conn)
			/*
			 * synchronize_rcu() was called just after deleting
			 * entry from the list, thus IO code path cannot
			 * change pointer back to the pointer which is going
			 * to be removed, we are safe here.
			 */
			continue;

		this_cpu_ptr(s_pcpu_stats)->io_found_dang_ptr_num++;

		/*
		 * We race with IO code path, which also changes pointer,
		 * thus we have to be careful not to overwrite it.
		 */
		if (xchg_conns(ppcpu_conn, conn, next))
			/*
			 * @ppcpu_conn was successfully replaced with @next,
			 * that means that someone could also pick up the
			 * @conn and dereferencing it right now, so wait for
			 * a grace period is required.
			 */
			wait_for_grace = true;
	}
	if (wait_for_grace) {
		synchronize_rcu();
		this_cpu_ptr(s_pcpu_stats)->io_doubl_sync_rcu_num++;
	}

	mutex_unlock(&s_conns_mutex);
}

static inline void __do_udelay(unsigned int delay_us)
{
	unsigned int d;

	while (delay_us) {
		d = min(delay_us, 1000u);
		udelay(d);
		delay_us -= d;
	}
}

static void run_io_work(struct work_struct *work)
{
	struct conn *conn;

	rcu_read_lock();
	conn = get_next_conn_rr();
	if (likely(conn)) {
		smp_rmb();
		WARN_ON(conn->deleted);
		this_cpu_ptr(s_pcpu_stats)->io_observed_valid_num++;
	} else
		this_cpu_ptr(s_pcpu_stats)->io_observed_nulls_num++;

	/* Emulate some load or interrupt in-between, max 2 ms */
	__do_udelay(prandom_u32_max(2000));

	if (likely(conn))
		WARN_ON(conn->deleted);
	rcu_read_unlock();

	if (!stop_please)
		/* Repeat */
		queue_work(s_wq, work);
}

static void run_add_work(struct work_struct *work)
{
	struct delayed_work *dwork = to_delayed_work(work);
	struct conn *conn;
	struct stats *s;
	int delay = msecs_to_jiffies(1);

	if (time_is_before_jiffies64(observed_null_jiffies +
				     HZ * THROTTLE_ADD_SECS)) {
		/*
		 * To have proper testing list should bounce from empty
		 * to filled.  Remove @conn from the list is always
		 * slower since it requires synchronize_rcu(), so give
		 * the list 10 seconds to fill in and then throttle.
		 */
		delay = msecs_to_jiffies(100);
		goto repeat;
	}

	conn = kzalloc(sizeof(*conn), GFP_KERNEL);
	if (!WARN_ON(!conn)) {
		add_conn_to_arr(conn);
		s = get_cpu_ptr(s_pcpu_stats);
		s->adds_num++;
		put_cpu_ptr(s_pcpu_stats);
	}
repeat:
	if (!stop_please)
		/* Repeat */
		queue_delayed_work(s_wq, dwork, delay);
}

static void run_rm_work(struct work_struct *work)
{
	struct delayed_work *dwork = to_delayed_work(work);
	struct conn *conn;

	mutex_lock(&s_rm_mutex);
	/* Just get the first element from the list for removal */
	conn = list_first_entry_or_null(&s_conns_list,
					typeof(*conn), entry);
	if (likely(conn)) {
		remove_conn_from_arr(conn);
		free_conn(conn);
		this_cpu_ptr(s_pcpu_stats)->rm_observed_valid_num++;
	} else {
		this_cpu_ptr(s_pcpu_stats)->rm_observed_nulls_num++;
		observed_null_jiffies = get_jiffies_64();
	}
	mutex_unlock(&s_rm_mutex);

	if (!stop_please)
		/* Repeat */
		queue_delayed_work(s_wq, dwork, 0);
}

static void rcu_rr_start(void)
{
	int cpu;

	/* Firstly stop if something is running */
	rcu_rr_stop();

	get_online_cpus();
	mutex_lock(&s_mutex);

	/* Reset stats */
	for_each_possible_cpu(cpu)
		memset(per_cpu_ptr(s_pcpu_stats, cpu), 0, sizeof(struct stats));

	/*
	 * Start IO works
	 */
	for_each_online_cpu(cpu) {
		struct work_struct *work = &s_io_works[cpu];

		INIT_WORK(work, run_io_work);
		queue_work_on(cpu, s_wq, work);
	}

	/*
	 * Start add works
	 */
	for_each_online_cpu(cpu) {
		struct delayed_work *work = &s_add_works[cpu];

		INIT_DELAYED_WORK(work, run_add_work);
		queue_delayed_work(s_wq, work, 0);
	}

	/*
	 * Start rm works
	 */
	for_each_online_cpu(cpu) {
		struct delayed_work *work = &s_rm_works[cpu];

		INIT_DELAYED_WORK(work, run_rm_work);
		queue_delayed_work(s_wq, work, 0);
	}

	mutex_unlock(&s_mutex);
	put_online_cpus();
}

static void rcu_rr_stop(void)
{
	mutex_lock(&s_mutex);
	stop_please = true;
	drain_workqueue(s_wq);
	free_conns_list();
	stop_please = false;
	mutex_unlock(&s_mutex);
}

static int __init rcu_rr_init(void)
{
	s_pcpu_conn = alloc_percpu(typeof(*s_pcpu_conn));
	if (WARN_ON(!s_pcpu_conn))
		return -ENOMEM;

	s_pcpu_stats = alloc_percpu(typeof(*s_pcpu_stats));
	if (WARN_ON(!s_pcpu_stats)) {
		free_percpu(s_pcpu_conn);
		return -ENOMEM;
	}
	s_wq = alloc_workqueue("rcu-rr-wq", 0, WQ_MAX_ACTIVE);
	if (WARN_ON(!s_wq)) {
		free_percpu(s_pcpu_conn);
		free_percpu(s_pcpu_stats);
		return -ENOMEM;
	}

	return 0;
}

static void __exit rcu_rr_exit(void)
{
	rcu_rr_stop();
	destroy_workqueue(s_wq);
	free_percpu(s_pcpu_stats);
	free_percpu(s_pcpu_conn);
}

module_init(rcu_rr_init);
module_exit(rcu_rr_exit);
