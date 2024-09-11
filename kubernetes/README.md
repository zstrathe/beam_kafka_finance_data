To setup locally with minikube:
- copy .kube/config and .minikube from host machine into airflow containers (worker, scheduler ... )
	- I needed to modify the paths to certificate files so I just copied these into the project directory, and mounted into container
- get minikube ip by running "minikube ip" on host machine
- set .kube/config minkikube cluster -> server: to <minikube ip>:8443
- Install the Spark Operator on the host PC with Helm (https://www.kubeflow.org/docs/components/spark-operator/getting-started/)
  run: 
 	helm install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace

- Add minikube docker network to airflow .yaml
- Setup Airflow connection for a "Kubernetes Cluster Connection" with config path matching path copied into Airflow container (e.g., /.kube/config if copied into root of container)
- run: 
	kubectl create namespace spark-operator
	kubectl create serviceaccount spark --namespace=spark-operator
	kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark-operator:spark --namespace=spark-operator


	kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark-operator:spark --namespace=spark-operator

