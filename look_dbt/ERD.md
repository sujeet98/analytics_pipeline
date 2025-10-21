```mermaid
erDiagram
    DISTRIBUTION_CENTERS ||--o{ PRODUCTS : "stocks/ships"
    DISTRIBUTION_CENTERS ||--o{ INVENTORY_ITEMS : "holds"
    USERS ||--o{ ORDERS : "places"
    ORDERS ||--o{ ORDER_ITEMS : "contains"
    PRODUCTS ||--o{ ORDER_ITEMS : "are in"
    PRODUCTS ||--o{ INVENTORY_ITEMS : "instantiated as"
    INVENTORY_ITEMS ||--o| ORDER_ITEMS : "fulfilled by"
    USERS ||--o{ EVENTS : "generates"

    DISTRIBUTION_CENTERS {
      int id PK
      string name
      float latitude
      float longitude
    }

    PRODUCTS {
      int id PK
      decimal cost
      string category
      string name
      string brand
      decimal retail_price
      string department
      string sku
      int distribution_center_id FK
    }

    USERS {
      int id PK
      string first_name
      string last_name
      string email
      int age
      string gender
      string state
      string street_address
      string postal_code
      string city
      string country
      float latitude
      float longitude
      string traffic_source
      datetime created_at
    }

    ORDERS {
      int order_id PK
      int user_id FK
      string status
      string gender
      datetime created_at
      datetime returned_at
      datetime shipped_at
      datetime delivered_at
      int num_of_item
    }

    ORDER_ITEMS {
      int id PK
      int order_id FK
      int user_id FK
      int product_id FK
      int inventory_item_id FK
      string status
      datetime created_at
      datetime shipped_at
      datetime delivered_at
      datetime returned_at
    }

    INVENTORY_ITEMS {
      int id PK
      int product_id FK
      datetime created_at
      datetime sold_at
      decimal cost
      string product_category
      string product_name
      string product_brand
      decimal product_retail_price
      string product_department
      string product_sku
      int product_distribution_center_id FK
    }

    EVENTS {
      int id PK
      int user_id FK
      int sequence_number
      string session_id
      datetime created_at
      string ip_address
      string city
      string state
      string postal_code
      string browser
      string traffic_source
      string uri
      string event_type
    }
