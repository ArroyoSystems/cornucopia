--! insert_item
INSERT INTO items (value, label) VALUES (:value, :label);

--! get_items: Item()
SELECT value, label FROM items ORDER BY value;
