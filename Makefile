INSTALL_DIR=`pwd`/../sqlite-install

all:
	cd sqlite && ./configure --prefix=${INSTALL_DIR} --disable-tcl
	cd sqlite && ${MAKE} ${MFLAGS} install

clean:
	rm -rf sqlite-install
	cd sqlite && ${MAKE} ${MFLAGS} distclean
