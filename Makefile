INSTALL_DIR=`pwd`/../sqlite_install

all:
	cd sqlite && ./configure --prefix=${INSTALL_DIR}
	cd sqlite && ${MAKE} ${MFLAGS} install

clean:
	rm -rf sqlite_install
	cd sqlite && ${MAKE} ${MFLAGS} clean
