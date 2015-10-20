# Alma.DigitalMigration
Scripts to migrate digital content to Alma

## Digitool
Use the scripts in this folder to migrate from Digitool.
### Installation Instructions
On any machine with [Ruby](https://www.ruby-lang.org/en/) and [Git](http://git-scm.com/) installed, do the following:
1. Clone this repository: `git clone https://github.com/ExLibrisGroup/Alma.DigitalMigration.git`
2. Install dependencies: `gem install rest-client nokogiri aws-sdk`
3. Define the variables contained in the `config.rb` file
4. Define AWS credentials with `[PROFILE]` as institution (see [here](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files) for more details)
5. Run the application: `ruby migrate.rb`