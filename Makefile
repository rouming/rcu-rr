# Called as kbuild
ifneq ($(KERNELRELEASE),)

obj-m += rcu-rr.o

# Normal Makefile, redirect to kbuild
else

KDIR ?= /lib/modules/`uname -r`/build

#
# ¯\(°_o)/¯ I dunno how to unite these two in one line
#

%:
	@$(MAKE) -C $(KDIR) M=$(PWD) $(MAKECMDGOALS)

all:
	@$(MAKE) -C $(KDIR) M=$(PWD) $(MAKECMDGOALS)

.PHONY: all

endif
