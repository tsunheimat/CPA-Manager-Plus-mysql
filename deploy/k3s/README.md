# k3s with MySQL

`cpa-manager-plus.yaml` runs one Manager and one MySQL 8.4 instance in namespace `cpamp`. MySQL has a 10 GiB persistent-volume claim; Manager has a separate 10 GiB claim for `data.key`, archives, and local configuration. These are starting requests, not workload sizing estimates. Both use k3s's [local-path storage](https://docs.k3s.io/add-ons/storage); this storage is tied to a node. Change the storage class and requested sizes before first deployment if you use another storage provider.

The Manager runs as UID/GID 10001 with a read-only root filesystem. `/data` is persistent and `/tmp` is writable temporary storage. Its `Recreate` strategy prevents overlapping Manager pods during an update. Keep `replicas: 1` even when using an external MySQL server.

## Build and publish the image

The repository's **PR Check** GitHub Actions workflow now also builds the image:

- Pull requests targeting `main`/`dev` and pushes to `dev` validate and build without publishing.
- Relevant pushes to `main` publish after the security, frontend, backend, manifest, and image smoke checks succeed.
- **Actions → PR Check → Run workflow** runs all checks. Select `main` and enable `publish_image` to publish manually. Publishing from another branch is rejected.

The workflow uses `GITHUB_TOKEN` with `packages: write`; it needs no Docker Hub credentials. It publishes both `linux/amd64` and `linux/arm64` to:

```text
ghcr.io/tsunheimat/cpa-manager-plus-mysql:main
ghcr.io/tsunheimat/cpa-manager-plus-mysql:sha-<full-commit-SHA>
```

The workflow builds and starts the amd64 image with real MySQL, UID 10001, and a read-only root. It checks authentication, the embedded panel, persistence after restart, and absence of a SQLite file. The arm64 image is cross-built. A seven-day `manager-image-<SHA>` artifact contains `image.json` and `image.env`, including the published digest. CI publishes images; it does not access or deploy to the cluster.

Push the workflow and application changes to the repository to activate this configuration. The existing upstream release workflows remain separate from this fork's continuous image publication.

## First deployment

1. Publish an image using the workflow above. For reproducible deployment, replace **both** Manager image references in `cpa-manager-plus.yaml` with the `ghcr.io/...@sha256:...` value from `image.env`. The supplied `:main` reference follows the latest successful main build when a pod starts.
2. Prepare credentials:

   ```sh
   cp deploy/k3s/secrets.example.yaml deploy/k3s/secrets.yaml
   chmod 600 deploy/k3s/secrets.yaml
   # Edit secrets.yaml and replace all three CHANGE_ME values.
   # Generate independent values with: openssl rand -hex 32
   ```

   `secrets.yaml` is excluded from Git and Docker's build context. The app password is stored once and used by both MySQL initialization and Manager's DSN. Keep the admin key for logging into the panel. MySQL's initialization credentials apply only to an empty data volume; changing the Secret alone does not rotate an existing database user's password.

3. Ensure the cluster can pull the GHCR image. A newly published package may be private even when the repository is public. Either make the package public or create a pull Secret using a token with `read:packages`:

   ```sh
   kubectl apply -f deploy/k3s/secrets.yaml
   kubectl -n cpamp create secret docker-registry ghcr-pull \
     --docker-server=ghcr.io --docker-username="$GHCR_USER" \
     --docker-password="$GHCR_READ_TOKEN"
   ```

   For a private package, add `imagePullSecrets: [{name: ghcr-pull}]` under the Manager Deployment's `spec.template.spec`. The reference covers its init container too. Public images need no pull Secret. See [GitHub's container registry authentication guidance](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

4. Apply and wait:

   ```sh
   kubectl apply -f deploy/k3s/secrets.yaml
   kubectl apply -f deploy/k3s/cpa-manager-plus.yaml
   kubectl -n cpamp rollout status statefulset/cpamp-mysql --timeout=300s
   kubectl -n cpamp rollout status deployment/cpa-manager-plus --timeout=300s
   kubectl -n cpamp port-forward service/cpa-manager-plus 18317:18317
   ```

   Open `http://127.0.0.1:18317/management.html`, enter the admin key, and configure the CPA connection. MySQL is internal to the cluster. The Manager Service is also internal; attach your existing Gateway/Ingress to `cpa-manager-plus:18317` for permanent access.

## Existing MySQL server

Remove the `cpamp-mysql` Service and StatefulSet documents from your deployment copy. Set `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, and `MYSQL_USER` in `cpamp-mysql-config`, and set `mysql-password` in the Secret. The Manager composes its DSN from those values. Add `?tls=true` to its `USAGE_DB_DSN` value for a remote server with a publicly trusted certificate. Provision the database and account using the grants in the [MySQL guide](../../docs/mysql.md).

For an existing SQLite installation, complete the [SQLite-to-MySQL migration](../../docs/mysql.md#move-an-existing-sqlite-installation) while Manager is stopped and transfer its original `data.key` and archives into the Manager volume. Applying this manifest by itself starts a fresh installation; it does not import SQLite data.

## Updates and backups

Back up MySQL and the Manager data volume together before updating. Native snapshot commands are described in the [MySQL operations guide](../../docs/mysql.md#operations-and-recovery). Run offline maintenance with Manager scaled to zero and a maintenance pod using the same image, environment, and data volume.

To update, copy `CPAMP_IMAGE` from the successful build's `image.env`, then set both container images:

```sh
kubectl -n cpamp set image deployment/cpa-manager-plus \
  manager="$CPAMP_IMAGE" wait-for-mysql="$CPAMP_IMAGE"
kubectl -n cpamp rollout status deployment/cpa-manager-plus --timeout=300s
```

Also update the saved YAML so a later `kubectl apply` retains that digest. A new `:main` push does not restart existing pods automatically. Keep the PVCs and namespace during updates. This manifest is a single-instance deployment, not database replication or automatic storage failover.
