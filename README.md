  # sql_tools

  Advanced SQL utilities from production analytics work — built to run heavy
  text-analysis workloads entirely inside the database using set-based logic
  instead of procedural loops or external code.

  > Adapted from working production queries to pseudocode (placeholder schema and
  > filters) to protect confidential data architecture.

  ## `text_block_word_grouping_analysis.sql`

  A pure-SQL engine that finds which words most frequently appear *together*
  across a large set of free-text records (originally customer-account comments,
  adaptable to any text field).

  **How it works**
  - Aggregates multi-line text into single records with `LISTAGG`
  - Tokenizes each block into indexed words *in SQL* via `CONNECT BY` +
    `REGEXP_SUBSTR` — no loops, no application code
  - Self-joins the token stream to generate every 3-word permutation
    (cross-join approach rather than iteration)
  - Canonicalizes each combination with `GREATEST`/`LEAST` so word order doesn't
    fragment the groups
  - Scores each group by frequency, weighted by average word-proximity and
    normalized against the max proximity per analytical group using window
    functions

  **Why set-based**
  Pushing tokenization and n-gram co-occurrence down to the engine (cross joins +
  analytics) instead of row-by-row loops is dramatically faster on large text
  volumes and portable across any grouping attribute.

  **Stack:** Oracle PL/SQL — `CONNECT BY`, `REGEXP_SUBSTR`, `LISTAGG`, analytic
  window functions

  **Usage:** set `customerlevelfilters` / `commentlevelfilters` to the attributes
  you want to slice by, point the source CTE at your text table, and run.
