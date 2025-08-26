use test;

-- 创建用户登录表
CREATE TABLE fact_log (
    user_id INT COMMENT '用户ID',
    device_id VARCHAR(50) COMMENT '设备ID',
    login_date DATE COMMENT '登录日期'
) COMMENT '用户登录记录表';

-- 插入10条测试数据
-- 包含不同用户的首次登录和后续登录场景
INSERT INTO fact_log (user_id, device_id, login_date) VALUES
    (101, 'd_001', '2023-10-01'),  -- 用户101首次登录
    (101, 'd_001', '2023-10-02'),  -- 用户101再次登录（老用户）
    (102, 'd_002', '2023-10-01'),  -- 用户102首次登录
    (103, 'd_003', '2023-10-02'),  -- 用户103首次登录
    (102, 'd_002', '2023-10-03'),  -- 用户102再次登录（老用户）
    (104, 'd_004', '2023-10-02'),  -- 用户104首次登录
    (105, 'd_005', '2023-10-03'),  -- 用户105首次登录
    (103, 'd_003', '2023-10-03'),  -- 用户103再次登录（老用户）
    (106, 'd_006', '2023-10-01'),  -- 用户106首次登录
    (101, 'd_001', '2023-10-03');  -- 用户101第三次登录（老用户）


SELECT
    login_date,
    -- 统计新用户数（登录日期等于首次登录日期）
    SUM(CASE WHEN fl.login_date = first_login THEN 1 ELSE 0 END) AS new_users,
    -- 统计老用户数（登录日期大于首次登录日期）
    SUM(CASE WHEN fl.login_date > first_login THEN 1 ELSE 0 END) AS old_users
FROM fact_log fl
-- 关联每个用户的首次登录日期
    JOIN (
    -- 计算每个用户的首次登录日期
        SELECT user_id, MIN(login_date) AS first_login
        FROM fact_log
        GROUP BY user_id
) user_first ON fl.user_id = user_first.user_id
-- 按登录日期分组统计
GROUP BY login_date
ORDER BY login_date;



