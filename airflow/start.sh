helm repo add apache-airflow https://airflow.apache.org
helm repo update

export NAMESPACE=airflow
export RELEASE_NAME=airflow

# Create namespace to install into.
kubectl ....

# Create a secret to be used for git sync
# 
# ..... recommendation : use airflow-ssh-secret (default), create a secret.yml --from-file :)!
#

# Adapt this command ; try to supply your custom values to this helm chart.
helm upgrade --install $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE .....