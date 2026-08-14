import pytest
from dbt.adapters.starrocks.impl import StarRocksAdapter


class TestDBTLikeIsSubmittableETL:
    @pytest.mark.parametrize(
        "sql, expected",
        [
            # DBT-like statements
            (
                """ create table `my_db`.`my_table`
                PRIMARY KEY (key1, key2, key3) PARTITION BY (`key1`)
                DISTRIBUTED BY HASH (another_key) BUCKETS 5 as 
                PROPERTIES (
                  "replication_num" = "1"
                )
                as with source as (
                    select * from `my_db`.`my_table`
                ),
                renamed as (
                    select
                        *
                    from source
                )
                select * from renamed
                """, True
            ),
            (
                """
                insert into `my_table`.`my_db` (`id`, `value`) values
                (%s,%s),(%s,%s),(%s,%s),(%s,%s)
                """,
                True
            ),
            (
                """
                cache select   *
                from `my_table`.`my_db`
                """,
                True
            ),
            # An optimizer hint between the keyword and the rest of the
            # statement must not hide the ETL from detection.
            (
                """
                insert /*+SET_VAR(dynamic_overwrite = TRUE, query_timeout = 900)*/
                overwrite `my_db`.`my_table` (`id`, `value`)
                (select `id`, `value` from `my_db`.`source`)
                """,
                True
            ),
            (
                "insert /*+SET_VAR(x = 1)*/ into `my_db`.`my_table` select 1",
                True
            ),
            # A leading dbt query comment must not hide it either.
            (
                """
                /* {"app": "dbt", "node_id": "model.p.m"} */
                insert overwrite `my_db`.`my_table` (`id`) (select 1)
                """,
                True
            ),
            # Still not submittable: a comment must not create a false match.
            (
                "/* insert overwrite fake */ select 1",
                False
            ),
        ]
    )
    def test_is_submittable_etl_suitable(self, sql, expected):
        assert StarRocksAdapter._is_submittable_etl(sql) == expected