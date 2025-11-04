WITH CTE AS (
    SELECT
        sessionId,
        ts
    FROM {{ source('raw', 'session_timestamp') }}
)
select
  sessionId,
  ts
from CTE