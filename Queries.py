import ConnectBigQuery as BQ

EVENT_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.events"
CUSTOMER_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.customers"
ORDERITEMS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_items"
ORDERPAYMENTS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_payments"
ORDER_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.orders"
PRODUCTS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.products"
PRODUCTCATEGORY_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.product_category_name_translation"
SELLERS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.sellers"
ORDERREVIEWS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_reviews"
GEOLOCATION_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.geolocation"





TOP_PRODUCTS = f"""
SELECT REPLACE(INITCAP(p.product_category_name),"_","") as ProductCategory, SUM(oi.price) AS Revenue
 FROM `{ORDER_TABLE}` oi
 INNER JOIN  `{PRODUCTS_TABLE}` p ON
 oi.product_id=p.product_id
 group by p.product_category_name
 order by Revenue desc LIMIT 10
 """
 
 
ORDER_TABLE_DETAILS= f"""SELECT order_id AS OrderId,
                    customer_id AS CustomerId,
                    order_status AS OrderStatus,
                    order_purchase_timestamp AS OrderPurchaseTimestamp,
                    order_approved_at AS OrderApprovedAt,
                    order_delivered_carrier_date AS OrderDeliveredCarrierDate,
                    order_delivered_customer_date AS OrderDeliveredCustomerDate,
                    order_estimated_delivery_date AS OrderEstimatedDeliveryDate FROM {ORDER_TABLE}"""
ORDERPAYMENTS_TABLE_DETAILS = f"""SELECT order_id AS OrderId,
                    payment_sequential AS PaymentSequential,
                    payment_type AS PaymentType,
                    payment_installments AS PaymentInstallments,
                    payment_value AS PaymentValue FROM {ORDERPAYMENTS_TABLE}"""
                    
PRODUCTS_TABLE_DETAILS = f"""SELECT product_id AS ProductId,
                    product_category_name AS ProductCategoryName,
                    product_name_lenght AS ProductNameLength,
                    product_description_lenght AS ProductDescriptionLength,
                    product_photos_qty AS ProductPhotosQty,
                    product_weight_g AS ProductWeightG,
                    product_length_cm AS ProductLengthCm,
                    product_height_cm AS ProductHeightCm,
                    product_width_cm AS ProductWidthCm FROM {PRODUCTS_TABLE}"""
                    
SELLERS_TABLE_DETAILS = f"""SELECT seller_id AS SellerId,
                    seller_zip_code_prefix AS SellerZipCodePrefix,
                    seller_city AS SellerCity,
                    seller_state AS SellerState FROM {SELLERS_TABLE}"""
                
CUSTOMER_TABLE_DETAILS = f"""SELECT customer_id AS CustomerId,
                    customer_unique_id AS CustomerUniqueId,
                    customer_zip_code_prefix AS CustomerZipCodePrefix,
                    customer_city AS CustomerCity,
                    customer_state AS CustomerState FROM {CUSTOMER_TABLE}"""
                    
customer_growth_query = f"""
                    SELECT DATE(order_purchase_timestamp) AS OrderDate, COUNT(DISTINCT customer_id) AS NewCustomers
                    FROM {ORDER_TABLE}
                    GROUP BY OrderDate ORDER BY OrderDate
                    """

seller_Revenue_query = f"""
                    SELECT s.seller_id AS SellerID, SUM(oi.price) AS Revenue
                    FROM {ORDERITEMS_TABLE} oi
                    JOIN {SELLERS_TABLE} s ON oi.seller_id = s.seller_id
                    GROUP BY s.seller_id ORDER BY Revenue DESC LIMIT 10
                    """
                    
TOP_PRODUCTS = f"""
                    SELECT REPLACE(INITCAP(p.product_category_name),"_","") as ProductCategory, SUM(oi.price) AS Revenue
                    FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_items` oi
                    INNER JOIN  `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.products` p ON
                    oi.product_id=p.product_id
                    group by p.product_category_name
                    order by Revenue desc LIMIT 10
                    """
WORST_PRODUCTS = f"""
                    SELECT REPLACE(INITCAP(p.product_category_name),"_","") as ProductCategory, SUM(oi.price) AS Revenue
                    FROM {ORDERITEMS_TABLE} oi
                    INNER JOIN  {PRODUCTS_TABLE} p ON
                    oi.product_id=p.product_id
                    group by p.product_category_name
                    order by Revenue LIMIT 10
                    """