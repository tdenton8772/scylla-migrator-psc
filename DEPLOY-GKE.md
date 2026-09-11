# Deploying on GKE with YuniKorn

Runs scylla-migrator as a Spark job on GKE, three executors scheduled by Apache
YuniKorn, writing to ScyllaDB Cloud through a Private Service Connect endpoint.

Read `README.md` first — this covers the cluster, not the connection.

---

## Before you start

The GKE cluster **must** sit in the VPC that has the PSC endpoint. Executors
open their own connections to ScyllaDB; it is not enough for the Spark driver
to be able to reach it.

Check both of these from any VM in that VPC:

    nslookup <your-endpoint>.clusters.scylla.cloud     # resolves to a 10.x address
    nc -zv <endpoint-ip> 9000                          # discovery port is open

If DNS does not resolve, the private DNS zone is not attached to this VPC. Fix
that before going further, or set `spark.scylla.psc.connectionAddr` to the
endpoint IP later.

You will need: `gcloud`, `kubectl`, `helm`, `docker`.

---

## 1. Create the cluster

Use **Standard**, not Autopilot — Autopilot will not run a custom scheduler.

Three nodes for three executors, plus room for the driver. Adjust the machine
type to your data volume; this is a starting point, not a recommendation.

    gcloud container clusters create migrator-spark \
      --project <PROJECT> --zone <ZONE> \
      --num-nodes 3 --machine-type e2-standard-8 --disk-size 100 \
      --network <VPC_WITH_PSC_ENDPOINT> --subnetwork <SUBNET> \
      --enable-ip-alias

    gcloud container clusters get-credentials migrator-spark \
      --project <PROJECT> --zone <ZONE>

---

## 2. Install YuniKorn

    helm repo add yunikorn https://apache.github.io/yunikorn-release
    helm repo update
    kubectl create namespace yunikorn

    helm install yunikorn yunikorn/yunikorn \
      --namespace yunikorn --version 1.9.0 \
      --set embedAdmissionController=false \
      --wait --timeout 8m

`embedAdmissionController=false` is deliberate. With the admission controller
on, YuniKorn rewrites the scheduler for every pod in the cluster, including
GKE's own system pods. Instead, Spark opts in per-pod with
`spark.kubernetes.scheduler.name=yunikorn`.

Check it:

    kubectl get pods -n yunikorn        # yunikorn-scheduler should be Running

---

## 3. Service account for Spark

    kubectl create namespace spark
    kubectl create serviceaccount spark -n spark
    kubectl create clusterrolebinding spark-role \
      --clusterrole=edit --serviceaccount=spark:spark

---

## 4. Build the image

The image needs two jars: the migrator assembly built with a Java driver that
supports client routes (see README step 2), and `psc-factory.jar`.

    mkdir build && cd build
    cp /path/to/scylla-migrator-assembly.jar .
    cp /path/to/psc-factory.jar .

    cat > Dockerfile <<'EOF'
    FROM apache/spark:4.0.2
    USER root
    COPY scylla-migrator-assembly.jar /opt/migrator/migrator.jar
    COPY psc-factory.jar              /opt/migrator/psc-factory.jar
    RUN chown -R 185:185 /opt/migrator
    USER 185
    EOF

Keep the jars out of `/opt/spark/jars` — that would force every shaded class in
the assembly onto Spark's own classpath.

    gcloud artifacts repositories create migrator \
      --project <PROJECT> --repository-format=docker --location <REGION>
    gcloud auth configure-docker <REGION>-docker.pkg.dev

    IMAGE=<REGION>-docker.pkg.dev/<PROJECT>/migrator/migrator:1
    docker buildx build --platform linux/amd64 -t $IMAGE --push .

`--platform linux/amd64` matters if you are building on an ARM Mac; GKE nodes
are amd64.

---

## 5. Store the migrator config

`config.yaml` holds database credentials, so put it in a Secret rather than a
ConfigMap or the image.

    kubectl create secret generic migrator-config \
      -n spark --from-file=config.yaml=./config.yaml

---

## 6. Submit the job

    ENDPOINT=$(gcloud container clusters describe migrator-spark \
      --project <PROJECT> --zone <ZONE> --format="value(endpoint)")

    spark-submit \
      --master k8s://https://$ENDPOINT \
      --deploy-mode cluster \
      --name migrator \
      --class com.scylladb.migrator.Migrator \
      --conf spark.kubernetes.authenticate.submission.oauthToken=$(gcloud auth print-access-token) \
      --conf spark.kubernetes.trust.certificates=true \
      --conf spark.kubernetes.namespace=spark \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.kubernetes.container.image=$IMAGE \
      --conf spark.executor.instances=3 \
      --conf spark.executor.cores=4 \
      --conf spark.executor.memory=8g \
      --conf spark.driver.memory=4g \
      --conf spark.kubernetes.driver.secrets.migrator-config=/opt/conf \
      --conf spark.kubernetes.scheduler.name=yunikorn \
      --conf spark.kubernetes.driver.label.applicationId=migrator-001 \
      --conf spark.kubernetes.executor.label.applicationId=migrator-001 \
      --conf spark.kubernetes.driver.label.queue=root.default \
      --conf spark.kubernetes.executor.label.queue=root.default \
      --jars local:///opt/migrator/psc-factory.jar \
      --conf spark.cassandra.connection.factory=com.scylladb.psc.ClientRoutesFactory \
      --conf spark.scylla.psc.connectionId=<YOUR_CONNECTION_ID> \
      --conf spark.scylla.config=/opt/conf/config.yaml \
      local:///opt/migrator/migrator.jar

Add `--conf spark.scylla.psc.hosts=<your-endpoint>.clusters.scylla.cloud` if the
migration source is a plain Cassandra cluster. Not needed for an Astra source.

No local Spark install? Run the same command out of the image:

    docker run --rm apache/spark:4.0.2 /opt/spark/bin/spark-submit <args...>

---

## 7. Confirm it used the private endpoint

**Exit code 0 does not mean it worked.** Check the executors — they are separate
JVMs and the driver looking healthy tells you nothing about them.

    kubectl get pods -n spark -o custom-columns=\
    'NAME:.metadata.name,STATUS:.status.phase,SCHEDULER:.spec.schedulerName'

Every pod should show `SCHEDULER: yunikorn`. Then:

    for p in $(kubectl get pods -n spark -o name | grep exec); do
      echo "$p"
      kubectl logs -n spark ${p#pod/} | grep -c '172\.'                    # want 0
      kubectl logs -n spark ${p#pod/} | grep -oE ':900[0-9]' | sort | uniq -c
    done

Good — no internal addresses, several per-node ports:

    0
       2 :9003
       2 :9005

Bad — the driver learned the cluster's internal addresses and could not reach
them, so everything funnelled through one connection:

    4
       6 :9000

Add `--conf spark.kubernetes.executor.deleteOnTermination=false` to keep
executor pods around for inspection after the run.

---

## Sizing

Start with `spark.executor.cores=4` and 8g per executor, then adjust on the
`connections` setting in `config.yaml` — that controls driver connections per
executor to ScyllaDB, and it multiplies by executor count. Three executors at
`connections: 16` is 48 connections to the target cluster.

Each ScyllaDB node is reached on its own port through the endpoint, so the
endpoint is not a single-connection bottleneck, but it is a single network path
— watch it before scaling executors aggressively.
