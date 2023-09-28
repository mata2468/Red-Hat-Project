import json
import os
from datetime import datetime, timedelta
from statistics import median
import csv



class data_extractor_to_json:
    def extract_time(self, time):
        # Convert a string timestamp to a datetime object
        return datetime.strptime(time, "%Y-%m-%d %H:%M:%S")

    # Print time gaps in the JSON file
    def _print_gaps(self, file_path="./our_data/json/joined_data/prom-label-proxy.json"): 
        with open(file_path, 'r') as f:
            data_json = json.load(f)
        for pod in data_json.keys():
            pod_data = data_json[pod]
            # items = sorted(pod_data.items(), key=lambda x: x[0])
            items = sorted(pod_data.items(), key=lambda x: extract_time(x[0]))
            for i in range(len(pod_data.keys()) - 1):
                current_timestamp = extract_time(items[i][0])
                next_timestamp = extract_time(items[i + 1][0])
                # print(current_timestamp, next_timestamp)
                time_diff = (next_timestamp - current_timestamp).total_seconds()

                if time_diff > 60:  # Assuming a gap is considered if it's more than 60 seconds
                    print("Gap found between {} and {} (Time diff: {} seconds)".format(
                        current_timestamp, next_timestamp, time_diff))

    # Calculate the number of points between two timestamps
    def calculate_num_of_points(self, start, stop):
        return int((stop - start).total_seconds() / 60)

    def debug_print(self, start, stop, start_cpu, stop_cpu, start_mem, stop_mem, start_cpu_index, stop_cpu_index, start_mem_index, stop_mem_index, num_of_points):
        print(f'start: {start}')
        print(f'stop: {stop}')
        print(f'start_cpu: {start_cpu}')
        print(f'stop_cpu: {stop_cpu}')
        print(f'start_mem: {start_mem}')
        print(f'stop_mem: {stop_mem}')


        print(f'start_cpu_index: {start_cpu_index}')
        print(f'stop_cpu_index: {stop_cpu_index}')
        print(f'start_mem_index: {start_mem_index}')
        print(f'stop_mem_index: {stop_mem_index}')
        print(f'num_of_points: {num_of_points}')

    def __init__(self, directory="data", application_name="prom-label-proxy", times=[], metric="cpu", is_zero=False):
        # initialize arguments
        for k, v in locals().items():
            setattr(self, k, v)
        self.output_file = f"./our_data/json/{metric}/{application_name}.json"

        # Arrays of times of the data jsons
        # cpu times:
        if self.metric == 'cpu':
            self.times = [
                ('04-19_17', '04-30_16'),
                ('04-30_16', '05-12_22'),
                ('05-12_22', '05-28_17'),
                ('05-28_17', '06-08_17'),
                ('06-08_17', '06-13_21'),
            ]
        else:
        # mem times:
            self.times = [
                ('04-19_17', '04-30_16'),
                ('04-30_16', '05-12_21'),
                ('05-12_21', '05-28_14'),
                ('05-28_14', '06-08_14'),
                ('06-08_14', '06-13_21'),
            ]
    
    # Extract points from a given time range
    def extract_points_from_time(self, start, stop, points):
            points_with_time_dict = {}
            for (delta, point) in enumerate(points):
                points_with_time_dict[f"{start + timedelta(minutes=delta)}"] = point

            return points_with_time_dict

        
    # Extract data from a JSON key
    def extract_from_key_json(self, key_json):
        application_dict = {}

        for data in key_json:
            application_dict.update(
                self.extract_points_from_time(
                    self.extract_time(data["start"]),
                    self.extract_time(data["stop"]),
                    data["data"]
                )
            )
        return application_dict


    # Extract data to a JSON format
    def extract_data_to_json(self, json_data):
        keys = [key for key in json_data.keys() if (self.application_name == key.split(", ")[0])]
        print(f'keys: {keys}')
        app_dict = {self.application_name: {key.split(", ")[3]: {} for key in keys}}
        for key in keys:
            pod = key.split(", ")[3] # pod name
            app_dict[self.application_name].update({pod: self.extract_from_key_json(json_data[key])})

        return app_dict

    # Update a JSON object with another JSON object
    def update_json(self, input_json, output_json):
        for key in input_json.keys():
            if key in output_json.keys():
                output_json[key].update(input_json[key])
            else:
                output_json[key] = input_json[key]
        return output_json

    # Find the last data point at a given time in a JSON object
    def last_point(self, json, time):
        while time not in json.keys():
            time = self.extract_time(time) - timedelta(minutes=1)
            time = str(time)
        return json[time]

    # Join CPU and memory data into a single JSON file
    def join_data(self, cpu_json, mem_json, output_file):
        with open(cpu_json, 'r') as f:
            cpu_json = json.load(f)
        with open(mem_json, 'r') as f:
            mem_json = json.load(f)

        joined_json = {
            pod: {} for pod in cpu_json[self.application_name].keys()
        }
        cpu_pods = cpu_json[self.application_name]
        mem_pods = mem_json[self.application_name]
        
        # Complete the time series
        for pod in cpu_pods.keys():
            for time in cpu_pods[pod].keys():
                if time in mem_pods[pod].keys():
                    joined_json[pod][time] = [cpu_pods[pod][time], mem_pods[pod][time]]
                    print("time exists:", time)
                else:
                    print(time)
                    joined_json[pod][time] = [cpu_pods[pod][time], 0 if self.is_zero else joined_json[pod][str(self.extract_time(time) - timedelta(minutes=1))][1]]
        # 2022-04-20 14:59:00
        # 2022-04-20 15:00:00
        # complete the times series of mem only times
        for pod in mem_pods.keys():
            for time in mem_pods[pod].keys():
                if time not in cpu_pods[pod].keys():
                    joined_json[pod][time] = [0 if self.is_zero else joined_json[pod][str(self.extract_time(time) - timedelta(minutes=1))][0], mem_pods[pod][time]]

        with open(output_file, 'w') as f:
            # write to json file
            f.write(json.dumps(joined_json, indent=4))

    # Fill single gap in data
    def fill_gap(self, updated_json, pod, current_datetime, stop_datetime): 
        mean_point = (updated_json[pod][str(current_datetime - timedelta(minutes=1))][0] + updated_json[pod][str(stop_datetime + timedelta(minutes=1))][0]) / 2   
        # Iterate over datetime objects between start and stop datetime
        while current_datetime <= stop_datetime:
            updated_json[pod][str(current_datetime)] = [0 if self.is_zero else mean_point, 0 if self.is_zero else mean_point]
            current_datetime += timedelta(minutes=1)

    # Fill all gaps in data
    def fill_all_gaps(self, json_file):
        with open(json_file, 'r') as json_file:
            data_json = json.load(json_file)

        updated_json = data_json
        pods = data_json.keys()
        for pod in pods:
            pod_data = data_json[pod]
            items = sorted(pod_data.items(), key=lambda x: self.extract_time(x[0]))
            for i in range(len(pod_data.keys()) - 1):
                current_timestamp = self.extract_time(items[i][0])
                next_timestamp = self.extract_time(items[i + 1][0])
                time_diff = (next_timestamp - current_timestamp).total_seconds()

                if time_diff > 60:  # Assuming a gap is considered if it's more than 60 seconds
                    print("Gap found between {} and {} (Time diff: {} seconds)".format(
                        current_timestamp, next_timestamp, time_diff))
                    self.fill_gap(updated_json, pod, current_timestamp, next_timestamp)
        
        with open(output_file, 'w') as f: # TODO: check
            # write to json file
            f.write(json.dumps(updated_json, indent=4))

    # Fill gaps in data JSON
    def fill_gap_data_json(self, updated_json, pod, current_datetime, stop_datetime): 
        while current_datetime <= stop_datetime:
            updated_json[self.application_name][pod][str(current_datetime)] = 0 if self.is_zero else updated_json[self.application_name][pod][str(current_datetime - timedelta(minutes=1))]
            current_datetime += timedelta(minutes=1)

    # Fill all gaps in a data JSON
    def fill_all_gaps_data_json(self, json_file):
        with open(json_file, 'r') as json_file_r:
            data_json = json.load(json_file_r)
        
        updated_json = data_json
        pods = data_json[self.application_name].keys()
        for pod in pods:
            pod_data = data_json[self.application_name][pod]
            items = sorted(pod_data.items(), key=lambda x: self.extract_time(x[0]))
            for i in range(len(pod_data.keys()) - 1):
                current_timestamp = self.extract_time(items[i][0])
                next_timestamp = self.extract_time(items[i + 1][0])
                time_diff = (next_timestamp - current_timestamp).total_seconds()

                if time_diff > 60:  # Assuming a gap is considered if it's more than 60 seconds
                    print("Gap found between {} and {} (Time diff: {} seconds)".format(
                        current_timestamp, next_timestamp, time_diff))

        output_name = json_file.replace('.json','_filled.json')
        with open(f'{output_name}.json', 'w') as f:
            # write to json file
            f.write(json.dumps(updated_json, indent=4))
    

    def aggregate_data(self, json_file, aggregate_func=median, output_file='./our_data/json/joined_data/aggregated_data/prom-label-proxy.json'):
        with open(json_file, 'r') as json_file:
            data_json = json.load(json_file)
        
        # aggregate points each 60 minutes with function
        aggregated_json = {pod: {} for pod in data_json.keys()}
        pods = data_json.keys()
        
        for pod in pods:
            pod_data = data_json[pod]
            points_size = len(data_json.keys())
            sorted_points = sorted(pod_data.items(), key=lambda x: self.extract_time(x[0]))
            for i in range(0, len(pod_data.keys()), 30):

                    points_to_aggregate = sorted_points[i:i+30]
                    cpu_points_to_aggregate = [point[1][0] for point in points_to_aggregate]
                    cpu_aggregated_point = aggregate_func(cpu_points_to_aggregate)
                    mem_points_to_aggregate = [point[1][1] for point in points_to_aggregate]
                    mem_aggregated_point = aggregate_func(mem_points_to_aggregate)
                    
                    aggregated_json[pod][points_to_aggregate[0][0]] = [cpu_aggregated_point, mem_aggregated_point]

        with open(output_file, 'w') as f:
            # write to json file
            f.write(json.dumps(aggregated_json, indent=4))
    
    def export_data_to_csv(self, json_file, csv_file_path=''):
        with open(json_file, 'r') as json_file:
            data_json = json.load(json_file)
        
        pods = data_json.keys()
        # pods = list(pods)
        # print(pods)
        # pods = pods[:-2]
        pods_array = []
        for pod in pods:
        
            pod_data = data_json[pod]
            pod_cpu_array = []
            pod_mem_array = []

            for time in pod_data:
                cpu = pod_data[time][0]
                mem = pod_data[time][1]
                pod_cpu_array.append(cpu)
                pod_mem_array.append(mem)

            pods_array.append(pod_cpu_array)
            pods_array.append(pod_mem_array)

        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(list(zip(*pods_array)))

    def main(self):

        output_json = {self.application_name: {}}
        for time1, time2 in self.times:
            json_file = f'container_{self.metric}_2022-{time1}_00_00_to_2022-{time2}_00_00.json'
            # json_file = 'cpu_test.json' # test
            file_path = os.path.join(self.directory, json_file)

            with open(file_path, "r") as json_data:
                    # extract cpu times
                    json_data = json.load(json_data)
            
            extracted_json = self.extract_data_to_json(json_data)
            
            # TODO: update file if exists
            # if os.path.exists(self.output_file):
            #     with open(self.output_file, 'r') as f:
            #         # write to json file
            #         json_data = json.load(f)
            #         # extracted_json[self.application_name].update(json_data[self.application_name])
            #         self.update_json(json_data[self.application_name], extracted_json[self.application_name])
            self.update_json(extracted_json[self.application_name], output_json[self.application_name])

        with open(self.output_file, 'w') as f:
            # write to json file
            f.write(json.dumps(output_json, indent=4))





# Example for running:

# all_app_names = {'github-receiver', 'kube-rbac-proxy-rules', 'tide', 'virt-api', 'jupyterbook-nlmmk5', 'ml-pipeline-viewer-crd', 'cdi-controller', 'etcdctl', 'tekton-pipelines-controller', 'kube-scheduler-recovery-controller', 'ml-pipeline-visualizationserver', 'cluster-baremetal-operator', 'coredns', 'kube-apiserver-operator', 'nmstate-cert-manager', 'coredns-monitor', 'csi-rbdplugin', 'ray-operator', 'nfd-worker', 'tekton-triggers-controller', 'cdi-apiserver', 'openshift-state-metrics', 'cluster-policy-controller', 'tekton-operator-webhook', 'telemeter-client', 'metal3-ironic-inspector', 'prom-label-proxy', 'grafana-proxy', 'exposer', 'user-operator', 'memory-over-utilization-sindhu-container', 'keepalived-monitor', 'keepalived', 'metal3-baremetal-operator', 'observatorium-loki-compactor', 'cert-controller', 'minio', 'sparkoperator', 'triage-party', 'rook-ceph-operator', 'ml-pipeline-api-server', 'katalog-connector', 'cdi-operator', 'nmstate-webhook', 'vector', 'mysql', 'csi-attacher', 'openshift-config-operator', 'osp-clf', 'openshift-acme', 'user-api', 'metal3-ironic-conductor', 'status-sync', 'metrics-exporter', 'api-designer-poc', 'peribolos-as-service', 'smart-village-view', 'webhook', 'kube-apiserver-cert-regeneration-controller', 'crier', 'machine-healthcheck-controller', 'web', 'console', 'cluster-image-registry-operator', 'pfcon', 'seldon-container-engine', 'packageserver', 'limitador', 'cni-plugins', 'machine-controller', 'management-api-openapi', 'klusterlet-addon-operator', 'notebook', 'container', 'step-pr-updates', 'tenant-manager', 'ml-pipeline-scheduledworkflow', 'machine-config-controller', 'pushgateway', 
# 'cluster-version-operator', 'final-presentation-rc-container', 'sinker', 'cloud-credential-operator', 'jupyterbook-bhdgfl-r-89m8m', 'swift', 'haproxy-monitor', 'virt-controller', 'postgresql-metrics-exporter', 'kafka-ui', 'envoy', 'kube-rbac-proxy-main', 'network-check-target-container', 'api', 'amun-api', 'core', 'noobaa-agent', 'olm-operator', 'dex', 'cpu-over-uti-container', 'external-provisioner', 'worker-periodic', 'postgres', 'dns', 'openshift-apiserver-check-endpoints', 'pytorch-inference-64d86edb', 'thanos-receive', 'meteor-559nz-jupyterbook', 'ocs-metrics-exporter', 'kube-multus-additional-cni-plugins', 'cert-policy-controller', 'cluster-node-tuning-operator', 'kube-scheduler-cert-syncer', 'sdn', 'snapshot-controller', 'controller-manager', 'nfd-master', 'pytorch-inference-d9e6bdfc', 'kube-controller-manager-cert-syncer', 'sefkhet-abwy-webhook-receiver', 'meteor-c6g85-jupyterbook', 'config-reloader', 'hyperconverged-cluster-operator', 'bridge-marker', 'kube-apiserver', 'cluster-autoscaler-operator', 'pulp-metrics-exporter', 'kube-scheduler-operator-container', 'influxdb', 'ocs-operator', 'package-server-manager', 'meteor-w7df9-jupyterbook', 'logfilesmetricexporter', 'config-sync-controllers', 'thanos-query-frontend', 'pulp-manager', 'kube-storage-version-migrator-operator', 'external-resizer', 'app', 'kube-multus', 'cloudbeaver', 'step-run-build', 'node-maintenance-operator', 'apicurio-registry-postgresql', 'cluster-storage-operator', 'ironic-deploy-ramdisk-logs', 'needs-rebase', 'kube-controller-manager', 'csi-resizer', 'topic-operator', 'endpoint', 'httpd', 'node-interface-labeler', 'etcd', 'kube-rbac-proxy-self', 'jupyterhub', 'machine-config-operator', 'kube-rbac-proxy', 'jupyterbook-bhdgfl', 'guard', 'reloader-reloader', 'kube-controller-manager-operator', 'server', 'marketplace-operator', 'observatorium-loki-query-frontend', 'klusterlet-manifestwork-agent', 'oauth-apiserver', 'jaeger', 'subscription-controller', 'haproxy', 'etcd-metrics', 'pytorch-inference-ac39d4a1', 'driver-registrar', 'tkn-cli-serve', 'hyperconverged-cluster-webhook', 'console-operator', 'selen-cpu-const-load-test', 'ironic-inspector-ramdisk-logs', 'openshift-apiserver-operator', 'chris-store-ui', 'queue', 'kube-mgmt', 'tls-sidecar', 'apicurio-registry-mem', 'opendatahub-operator', 'cleanup', 'jupyterhub-ha-sidecar', 'grafana', 'etcd-health-monitor', 'prow-controller-manager', 'postgresql', 'oauth-openshift', 'openshift-controller-manager-operator', 'configmap-puller', 'demo-clf', 'argo-server', 'jupyterhub-idle-culler', 'kube-rbac-proxy-machineset-mtrc', 'tuned', 'template-sync', 'proxy', 'apicurio-bot', 'pytorch-inference-064d6f8e', 'github-pr-ttm-clf', 'investigator-consumer', 'metal3-httpd', 'camel-k-operator', 'slack-first', 'metal3-mariadb', 'ghproxy', 'network-metrics-daemon', 'ray-node', 'curator-reports', 'cluster-samples-operator-watch', 
# 'jupyterbook-8qi3bv', 'vm-import-operator', 'check-endpoints', 'ml-pipeline-ui', 'cluster-network-addons-operator', 'registry', 'strimzi-cluster-operator', 'virt-launcher', 'cert-manager', 'download-server', 'external-secrets', 'tekton-triggers-core-interceptors', 'acm-agent', 'iam-policy-controller', 'telegraf', 'kube-state-metrics', 'metastore', 'blackbox-exporter', 'meteor-jslzb-jupyterbook', 'statusreconciler', 'scheduler', 'pytorch-inference-50fefab3', 'compute', 'nmstate-handler', 'meteor-6hn5n-jupyterbook', 'node-ca', 'sin-mem-under-uti-container', 'csi-snapshotter', 'thanos-ruler-proxy', 'meteor-9jh2c-jupyterbook', 'cluster-monitoring-operator', 'registry-server', 'message-metrics', 'alertmanager-proxy', 'ingress', 'kube-rbac-proxy-mhc-mtrc', 'vertical-pod-autoscaler-operator', 'kube-apiserver-insecure-readyz', 'meteor-brpkp-jupyterbook', 'workflow-controller', 'csi-provisioner', 'odh-dashboard', 'openshift-apiserver', 'tekton-dashboard', 'meteor-cl5kz-jupyterbook', 'cdi-uploadproxy', 'jaeger-operator', 'machine-api-operator', 'migrator', 'oauth-proxy', 'machine-config-server', 'hostpath-provisioner-operator', 'router', 'insights-puptoo', 'wait', 'superset-init', 'content', 'solr', 'etcd-operator', 'serve-healthcheck-canary', 'virt-operator', 'manager', 'metal3-ironic-api', 'network-operator', 'superset', 'cluster-cloud-controller-manager', 'klusterlet', 'cpu-under-uti-container', 'sdn-controller', 'k8s-annotations-exporter', 'opa-connector', 'apicurio-registry', 'ads-ui', 'meteor-wj4q4-jupyterbook', 'thanos-receive-controller', 'meteor-25tkb-jupyterbook', 'sefkhet-abwy-chatbot', 'config-policy-controller', 'ingress-operator', 'shower', 'arrow-flight-module-chart', 'csi-snapshot-controller-operator', 'cluster-logging-operator', 'nginx', 'virt-handler', 'hook', 'controller', 'service-ca-controller', 'apicurio-registry-mt-ui', 'service-ca-operator', 'github-labeler', 'dns-operator', 'thanos-compact', 'kube-apiserver-cert-syncer', 'db', 'vault', 'kube-controller-manager-recovery-controller', 'nodelink-controller', 'machine-approver-controller', 'kube-apiserver-check-endpoints', 'branchprotector', 'vm-import-controller', 'classifier', 'insights-operator', 'machineset-controller', 'trino-worker', 'meteor-hbk8z-jupyterbook', 'meteor-26nt9-jupyterbook', 'pgbouncer', 'collector', 'meteor-qlsp8-jupyterbook', 'worker', 'spec-sync', 'chris-ui', 'horologium', 'meteor-shower', 'exporter', 'multus-admission-controller', 'csi-driver', 
# 'traefik-proxy', 'vertical-pod-autoscaler', 'ml-pipeline-persistenceagent', 'opa', 'ssh-access', 'alertmanager', 'meteor-v99q2-jupyterbook', 'meteor-dngcb-jupyterbook', 'studio-editors', 'dns-node-resolver', 'localstack', 'chris', 'authentication-operator', 'thanos-store', 'machine-config-daemon', 'cluster-samples-operator', 'rook-ceph-tools', 'kube-rbac-proxy-machine-mtrc', 'thanos-ruler', 'meteor-fp54s-jupyterbook', 'tekton-chains-controller', 'node-exporter', 'memcached', 'chris-db', 'observatorium-loki-ingester', 'observatorium-loki-distributor', 'observatorium-api', 'zookeeper', 'meteor-6q7vb-jupyterbook', 'wait-for-database', 'reload', 'kafka', 'redis', 'registration-controller', 'mosquitto-ephemeral', 'observatorium-loki-querier', 'mosquitto', 'kube-scheduler', 'catalog-operator', 'event-listener', 'openshift-pipelines-operator', 'trino-coordinator', 'deck', 'thanos-query', 'main', 'noobaa-operator'}

data_extractor_to_json(metric='cpu', application_name='prom-label-proxy').main()
Data_extractor_to_json = data_extractor_to_json(metric='cpu', application_name='prom-label-proxy', is_zero=False) 
cpu_json = './our_data/json/cpu/prom-label-proxy.json'
mem_json = './our_data/json/mem/prom-label-proxy.json'
output_file = './our_data/json/joined_data/prom-label-proxy-avg.json'

file_json = './our_data/json/cpu/prom-label-proxy.json'
Data_extractor_to_json.fill_all_gaps_data_json(file_json)

# Data_extractor_to_json.join_data(cpu_json, mem_json, output_file)

# Data_extractor_to_json.fill_all_gaps(output_file)

# Data_extractor_to_json.aggregate_data(output_file)

# json_file = './our_data/json/joined_data/aggregated_data/prom-label-proxy.json'
# data_extractor_to_json(metric='cpu', application_name='prom-label-proxy', is_zero=True).export_data_to_csv(json_file, './our_data/csv/prom-label-proxy-joined.csv')

print("Data conversion complete.")
