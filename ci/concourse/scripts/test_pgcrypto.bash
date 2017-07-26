#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
  cat > /opt/run_test.sh <<-EOF

		TINCDIR="\${1}/gpdb_src/src/test/tinc"
		trap look4diffs ERR
		function look4diffs() {
		find "\${TINCDIR}" -name *.diff -exec cat {} \; >> "\${TINCDIR}/regression.diffs"
		echo "=================================================================="
		echo "The differences that caused some tests to fail can also be viewed in the file saved at \${TINCDIR}/regression.diffs."
		echo "=================================================================="
		cat "\${TINCDIR}/regression.diffs"
		exit 1
		}

		if [ -f /opt/gcc_env.sh ]; then
		source /opt/gcc_env.sh
		fi
		source /usr/local/greenplum-db-devel/greenplum_path.sh
		pushd "\${1}/gpdb_src/gpAux"
		source gpdemo/gpdemo-env.sh
		popd

		PGDATABASE=${PGDATABASE:=gptest}
		createdb ${PGDATABASE}
		export PGDATABASE="${PGDATABASE}"
		
		echo $PGDATABASE
		pushd "\${1}/pgcrypto_gppkg"
		gppkg -i pgcrypto*.gppkg
		psql -d ${PGDATABASE} -f ${GPHOME}/share/postgresql/contrib/pgcrypto.sql
		popd
		
		
		pushd "\${1}/gpdb_src/src/test/tinc"
		source tinc_env.sh
		make pgcrypto
		popd
	EOF

  chmod a+x /opt/run_test.sh
}

function _main() {

    if [ -z "$TEST_OS" ]; then
        echo "FATAL: TEST_OS is not set"
        exit 1
    fi

    if [ "$TEST_OS" != "centos" -a "$TEST_OS" != "sles" ]; then
        echo "FATAL: TEST_OS is set to an invalid value: $TEST_OS"
        echo "Configure TEST_OS to be centos or sles"
        exit 1
    fi

    time install_gpdb
    time ./gpdb_src/ci/concourse/scripts/setup_gpadmin_user.bash "$TEST_OS"

    #set the connection limit to 250 and segment pairs to 2 for TINC tests
    export DEFAULT_QD_MAX_CONNECT=250
    export NUM_PRIMARY_MIRROR_PAIRS=2
    su gpadmin -c "echo $HOSTNAME:localhost > ~gpadmin/.gphostcache"
    time make_cluster

    time gen_env
    time run_test
}

_main "$@"

