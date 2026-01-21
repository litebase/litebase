# Instructions

- Write documentation in markdown in the `docs` directory under the relevant subdirectory using the package name (auth, database, etc.) as a guide.
- Use headings, lists, code blocks, tables, and lists as needed to organize the information clearly.
- Create files for different components or concepts as needed.
- Create a general overview file and specific files for individual components or concepts.
- Create an index.md file in each subdirectory that links to the other files in that directory.
- Create an overview.md that describes the high-level architecture and main components of the package.
- Read the code of a file or package and explain what the different components are,
  how they work together, and technical details about their implementation.
- Focus the writing on clarity and completeness, ensuring that someone unfamiliar with the code
  can understand its purpose and functionality.
- Litebase is mostly accessed via its API and CLIs, so explanations should often include
  how to use these interfaces effectively. Adding code examples of internal APIs may not be necessary.
- Do not just point to where the code is, describe what the code does.
- Run `npx markdownlint-cli "**/*.md"` and fix all reported issues on modified files.
