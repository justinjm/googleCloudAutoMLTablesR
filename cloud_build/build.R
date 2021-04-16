library(googleCloudRunner)

cr_setup()

cr_deploy_pkgdown(
  "justinjm/googleCloudAutoMLTablesR",
  secret = "github-ssh-justinjm",
  create_trigger = "inline"
)
