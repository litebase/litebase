# Instructions

- Write tests for all new functionality added to the codebase.
- When an agent makes changes to existing functionality, ensure that the relevant tests are updated accordingly.
- Run the tests for the file during development and the full package before confirming the changes were made to ensure everything is working as expected.
- When running a test that needs to have a timeout always use go the `-timeout` flag to avoid hanging tests no the `timeout` command.
- When running a test that needs to be ran muliple times use the `-count` flag to avoid false positives due to caching not a loop.
