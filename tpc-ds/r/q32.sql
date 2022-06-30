select sum(cs_ext_discount_amt) as "excess discount amount"
from catalog_sales
   , item
   , date_dim
where i_manufact_id = 269
  and i_item_sk = cs_item_sk
  and cs_sold_date_sk >= (select d_date_sk from date_dim where d_date = '1998-03-18')
  and cs_sold_date_sk <=
      (select d_date_sk from date_dim where d_date = date_add(cast('1998-03-18' as date), interval 90 day))
  and d_date_sk = cs_sold_date_sk
  and cs_sold_date_sk >= (select d_date_sk from date_dim where d_date = '1998-03-18')
  and cs_sold_date_sk <=
      (select d_date_sk from date_dim where d_date = date_add(cast('1998-03-18' as date), interval 90 day))
  and cs_ext_discount_amt
    > (select 1.3 * avg(cs_ext_discount_amt)
       from catalog_sales
          , date_dim
       where cs_item_sk = i_item_sk
         and cs_sold_date_sk >= (select d_date_sk from date_dim where d_date = '1998-03-18')
         and cs_sold_date_sk <=
             (select d_date_sk from date_dim where d_date = date_add(cast('1998-03-18' as date), interval 90 day))
         and d_date_sk = cs_sold_date_sk)
limit 100
;





