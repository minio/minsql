#/bin/sh -f
set -e

if [ ! -f "$HYPERSCAN_ROOT/lib/libhs.a" ]; then
	wget https://github.com/01org/hyperscan/archive/v$HYPERSCAN_VERSION.tar.gz -O /tmp/hyperscan.tar.gz
	tar -xzf /tmp/hyperscan.tar.gz
	cd hyperscan-$HYPERSCAN_VERSION
	rm -rf tools
	if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
		cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo \
				-DBOOST_ROOT=$BOOST_ROOT \
				-DCMAKE_POSITION_INDEPENDENT_CODE=on \
				-DCMAKE_INSTALL_PREFIX=$HYPERSCAN_ROOT
	else
		cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo \
				-DBOOST_ROOT=$BOOST_ROOT \
				-DCMAKE_POSITION_INDEPENDENT_CODE=on \
				-DCMAKE_INSTALL_PREFIX=$HYPERSCAN_ROOT \
				-DCMAKE_C_COMPILER=/usr/bin/gcc-4.8 \
				-DCMAKE_CXX_COMPILER=/usr/bin/g++-4.8
	fi
	make
	make install
else
	echo "Using cached hyperscan v${HYPERSCAN_VERSION} @ ${HYPERSCAN_ROOT}.";
fi
