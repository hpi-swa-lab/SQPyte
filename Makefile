INSTALL_DIR=`pwd`/../sqlite-install

all:
	cd sqlite && CFLAGS="-DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_OMIT_PROGRESS_CALLBACK" ./configure --prefix=${INSTALL_DIR} --disable-tcl --disable-threadsafe
	cd sqlite && ${MAKE} ${MFLAGS} install

clean:
	rm -rf sqlite-install
	cd sqlite && ${MAKE} ${MFLAGS} distclean
