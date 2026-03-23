{% snapshot users_snapshot %}
{{
  config(
    target_schema='snapshots',
    strategy='check',
    unique_key='user_id',
    check_cols='all',
  )
}}
select * from {{ ref('users') }}
{% endsnapshot %}
