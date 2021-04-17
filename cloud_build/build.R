library(googleCloudRunner)

# Create build trigger --------------------------------------------------------
## to automatically build pkgdown website when committing code to GitHubDD

## Create custom build steps -----------------------------------------------------
yaml <- cr_build_yaml(
  steps = c(
    cr_buildstep_secret(
      id = "download service account json for authenticated vingettes",
      "gc-automl-tables-r-service-account",
      decrypted = "/workspace/gc-automl-tables-r-291b441d9ddd.json"
    ),

    cr_buildstep_pkgdown(
      github_repo = "justinjm/googleCloudAutoMLTablesR",
      git_email = "justin@harborislandanalytics.com",
      secret = "github-ssh-justinjm",
      env = c("GAR_SERVICE_JSON=/workspace/gc-automl-tables-r-291b441d9ddd.json",
              "GCAT_DEFAULT_PROJECT_ID=gc-automl-tables-r")
    )
  )
)

## print to sanity check
print(yaml)

bb <- cr_build_make(yaml = yaml)

github <- cr_buildtrigger_repo("justinjm/googleCloudAutoMLTablesR", branch = "master")

cr_buildtrigger(bb,
                name = "cr-deploy-pkgdown-20210417",
                trigger = github,
                description = "Build pkgdown website on master branch",
                ignoredFiles = c("docs/**", "inst/**", "tests/**"))


# source: https://code.markedmondson.me/googleCloudRunner/articles/cloudbuild.html#build-triggers-via-code-1
