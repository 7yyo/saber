select i_item_id
     , i_item_desc
     , i_category
     , i_class
     , i_current_price
     , sum(ws_ext_sales_price)                                                                  as itemrevenue
     , sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price)) over (partition by i_class) as revenueratio
from web_sales
   , item
   , date_dim
where ws_item_sk = i_item_sk
  and i_category in ('Jewelry', 'Sports', 'Books')
  and ws_sold_date_sk = d_date_sk
  and ws_sold_date_sk >= (select d_date_sk from date_dim where d_date = cast('2001-01-12' as date))
  and ws_sold_date_sk <=
      (select d_date_sk from date_dim where d_date = date_add(cast('2001-01-12' as date), interval 30 day))
group by i_item_id
       , i_item_desc
       , i_category
       , i_class
       , i_current_price
order by i_category
       , i_class
       , i_item_id
       , i_item_desc
       , revenueratio
limit 100
;