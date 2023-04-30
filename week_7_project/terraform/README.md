# How to run terraform
make sure you grant service account a permission to access gcp and save it to
some path and set env-var `GOOGLE_APPLICATION_CREDENTIALS` refer to that path

```bash
export GOOGLE_APPLICATION_CREDENTIALS= "<path to your key>"

# example for me
export GOOGLE_APPLICATION_CREDENTIALS="/c/Users/Nattawat/.google/credentials/google_credentials.json"
```

```bash
terraform init
```

```bash
terraform plan
```

```bash
terraform apply
```

# How to remove service in gcp

```bash
export GOOGLE_APPLICATION_CREDENTIALS= "<path to your key>"

# example for me
export GOOGLE_APPLICATION_CREDENTIALS="/c/Users/Nattawat/.google/credentials/google_credentials.json"
```

```bash
terraform destroy
```
# video

https://www.loom.com/share/3e8ed0189e744131b3751e20c238534a

# ref

[How to install terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

[How to create service account gcp](https://cloud.google.com/iam/docs/service-accounts-create)