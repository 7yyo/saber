with ssci as (select ss_customer_sk customer_sk
                   , ss_item_sk     item_sk
              from store_sales,
                   date_dim
              where ss_sold_date_sk = d_date_sk
              and ss_sold_date_sk >= (select min(d_date_sk) from date_dim where d_month_seq between 1212 and 1212 + 11)
              and ss_sold_date_sk <= (select max(d_date_sk) from date_dim where d_month_seq between 1212 and 1212 + 11)
              group by ss_customer_sk
                     , ss_item_sk),
     csci as (select cs_bill_customer_sk customer_sk
                   , cs_item_sk          item_sk
              from catalog_sales,
                   date_dim
              where cs_sold_date_sk = d_date_sk
                and cs_sold_date_sk >= (select min(d_date_sk) from date_dim where d_month_seq between 1212 and 1212 + 11)
                and cs_sold_date_sk <= (select max(d_date_sk) from date_dim where d_month_seq between 1212 and 1212 + 11)
              group by cs_bill_customer_sk
                     , cs_item_sk)
select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end)     store_only
     , sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end)     catalog_only
     , sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci
         right join csci
                    on (ssci.customer_sk = csci.customer_sk
                        and ssci.item_sk = csci.item_sk)
limit 100
;

