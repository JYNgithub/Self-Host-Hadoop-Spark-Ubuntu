SELECT e.*, u.*
FROM app_data.expense_data e
INNER JOIN app_data.user_data u
ON e.user_id = u.user_id
LIMIT 10;
