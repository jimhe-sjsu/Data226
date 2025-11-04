{{ config(materialized='view') }}

select
  userId,
  sessionId,
  channel
from {{ source('raw', 'user_session_channel') }}