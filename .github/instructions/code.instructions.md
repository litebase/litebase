# Instructions

- Always write Go code following these spacing rules:
  - Add **one blank line** before `if`, `for`, `switch` statements and after variable declarations inside functions.
  - Example (correct):

    ```
    // Bad (no space before if):
    a, err := b()
    if err == nil && !a {
      // do something
    }

    // Good (space added before if):
    a, err := b()

    if err == nil && !a {
      // do something
    }
    ```

  - Apply this spacing consistently to all control flow blocks.
  - A comment before a control flow block does not require an additional blank line.

- Include comments explaining function purpose and complex logic.
