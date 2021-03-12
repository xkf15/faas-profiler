./WorkloadInvoker -c workload_configs_hello.json
printf '\xF0\xF0\xF0\xF0' | sudo dd bs=8 status=none of=/dev/pqii_pci count=1 seek=20
