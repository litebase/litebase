# Markdown Formatting Instructions

Follow these rules when writing documentation to pass `npx markdownlint-cli` without errors.

## Blank Lines (MD022, MD031, MD032)

**Always add a blank line BEFORE:**

- Level 1, 2, and 3 headings (`#`, `##`, `###`)
- Code fences (both opening and closing ` ``` `)
- Lists (bullet and numbered)

**Always add a blank line AFTER:**

- Code fences (closing ` ``` `)
- Heading content that precedes a list or code block

### Examples

```markdown
# CORRECT: Blank line before heading

Some text.

### Subheading

Some more text.
```

```markdown
# INCORRECT: No blank line before heading

Some text.

### Subheading

This will fail MD022.
```

## Code Blocks (MD040)

**Always specify a language identifier** in code fence opening:

````markdown
# CORRECT

```bash
echo "hello"
```
````

```go
type MyStruct struct {}
```

```json
{ "key": "value" }
```

````

```markdown
# INCORRECT - no language
````

echo "hello"

```

This fails MD040. Specify: bash, go, json, text, sql, etc.
```

## Line Length (MD013)

**Keep lines under 80 characters.** Break long lines across multiple lines:

```markdown
# CORRECT

Main entry point for all auth operations. Manages three credential managers
and coordinates credential lookups.

# INCORRECT

Main entry point for all auth operations. Manages three credential managers and coordinates credential lookups.
```

For long URLs or code snippets that can't be broken, use a continuation line:

```markdown
See [authentication middleware](../../pkg/http/
authentication_middleware.go) for details.

curl -H "Authorization: Litebase-HMAC-SHA256 \
credential=acc_123,signed_headers=host,signature=..."
```

## Heading Rules

**No trailing punctuation (MD026):**

```markdown
# CORRECT: Database Operations

# INCORRECT: Database Operations:
```

**No duplicate headings (MD024):**

If you have multiple sections with the same operations (Create, List, Show),
prefix with context:

```markdown
# CORRECT

## Access Keys

### Access Keys: Create

### Access Keys: List

## Tokens

### Tokens: Create

### Tokens: List

# INCORRECT

## Access Keys

### Create

### List

## Tokens

### Create

### List
```

## List Formatting (MD032)

**Add blank lines around lists:**

```markdown
# CORRECT

Some text describing the list.

- Item 1
- Item 2
- Item 3

More text after the list.

# INCORRECT

Some text describing the list.

- Item 1
- Item 2

More text fails MD032.
```

## Multiple Blank Lines (MD012)

**Never use more than one consecutive blank line.** Remove extra spacing:

```markdown
# CORRECT

Paragraph one.

Paragraph two.

# INCORRECT

Paragraph one.

Paragraph two.
```

## Trailing Spaces (MD009)

**Remove all trailing whitespace** at the end of lines.

Use your editor's trim trailing whitespace feature.

## Table Formatting (MD060)

**Use consistent table pipes with proper spacing:**

```markdown
# CORRECT

| Header 1 | Header 2 |
| -------- | -------- |
| Cell 1   | Cell 2   |
| Cell 3   | Cell 4   |

# INCORRECT (uneven pipes)

| Header 1 | Header 2 |
| -------- | -------- |
| Cell 1   | Cell 2   |
```

## Quick Checklist

Before running markdownlint:

- [ ] Blank line before every heading
- [ ] Blank line before and after every code fence
- [ ] All code fences have language identifier (bash, go, json, etc.)
- [ ] No lines exceed 80 characters
- [ ] No duplicate heading names (use context prefixes if needed)
- [ ] No trailing punctuation on headings
- [ ] Blank lines around all lists
- [ ] Only one blank line between paragraphs
- [ ] No trailing spaces
- [ ] Tables use consistent pipe formatting

## Running the Linter

```bash
# Check all markdown files
npx markdownlint-cli "**/*.md"

# Check specific directory
npx markdownlint-cli "docs/auth/*.md"

# Fix common issues automatically
npx markdownlint-cli --fix "docs/**/*.md"
```

Exit code `0` = all files pass. Exit code `1` = errors found.

## Resources

- [markdownlint Rules](https://github.com/DavidAnson/markdownlint/blob/main/doc/Rules.md)
- [Markdown Syntax Guide](https://www.markdownguide.org/)
