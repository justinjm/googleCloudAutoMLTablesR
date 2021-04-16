library(googleCloudRunner)

# Create build trigger --------------------------------------------------------
## to automatically build pkgdown website when committing code to GitHubDD
cr_deploy_pkgdown(
  "justinjm/googleCloudAutoMLTablesR",
  secret = "github-ssh-justinjm",
  create_trigger = "inline"
)

