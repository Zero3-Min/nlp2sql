专为 **医院信息统计系统（9 张表）** 设计的
**Text2SQL 单表查询测试题集（自然语言问题 + 对应正确 SQL）**。

本题集基于前面定义的坑点方向，覆盖 **9 大查询类型**（单指标、多指标、过滤、分组、排名、时间序列、比较、占比、汇总、报表展示），
共计 **45 个高质量测试样例**，每类约 5 题，适合用于 **Text2SQL 模型微调或评测**。

---

# 🩺 **1️⃣ 单指标查询（Single Metric）**

📘 定义： 查询单一字段或单一指标（如数量、平均值、最大值）。

⚠️ 坑点方向
| 坑点类型      | 说明                                  |
| --------- | ----------------------------------- |
| 同义混淆      | “人数”、“数量”、“条数” → COUNT()            |
| NULL 计数错误 | COUNT(column) 会忽略 NULL；COUNT(*) 不会  |
| 聚合误用      | SELECT COUNT(*) + GROUP BY 无 HAVING |
| 逻辑误判      | SUM() 误当作 COUNT()                   |
| 类型不匹配     | DECIMAL 与 INT 聚合结果精度丢失              |


| 自然语言问题           | 正确 SQL                                  |
| ---------------- | --------------------------------------- |
| 查询医生总人数是多少？      | `SELECT COUNT(*) FROM doctor_info;`     |
| 当前医院共有多少个科室？     | `SELECT COUNT(*) FROM department_info;` |
| 护士表中共有多少条记录？     | `SELECT COUNT(*) FROM nurse_info;`      |
| 医生表中最高的月薪是多少？    | `SELECT MAX(salary) FROM doctor_info;`  |
| 病人表中最年轻病人的年龄是多少？ | `SELECT MIN(age) FROM patient_info;`    |

---

# 📊 **2️⃣ 多指标查询（Multi Metric）**

📘 定义： 同时计算多个指标（如人数 + 平均工资 + 最大值）。

| 坑点     | 说明                                       |
| ------ | ---------------------------------------- |
| 聚合冲突   | SELECT COUNT(*), AVG(salary) 没有 GROUP BY |
| 列别名冲突  | AS 后重名导致后续引用错误                           |
| 单位混用   | 不同字段量纲不同（床位数 vs 薪资）                      |
| 精度丢失   | AVG() 默认返回 DOUBLE，易误差                    |
| 冗余重复计算 | 子查询内再聚合导致重复统计                            |


| 自然语言问题             | 正确 SQL                                                                                                          |
| ------------------ | --------------------------------------------------------------------------------------------------------------- |
| 统计每个职称医生的人数和平均工资   | `SELECT title, COUNT(*) AS doctor_count, AVG(salary) AS avg_salary FROM doctor_info GROUP BY title;`            |
| 医院表中各等级的医院数量与平均床位数 | `SELECT level, COUNT(*) AS num_hospitals, AVG(total_beds) AS avg_beds FROM hospital_info GROUP BY level;`       |
| 查询各科室的医生人数与最高薪资    | `SELECT dept_id, COUNT(*) AS num_doctors, MAX(salary) AS max_salary FROM doctor_info GROUP BY dept_id;`         |
| 统计护士表中白班与夜班的平均薪资   | `SELECT shift, AVG(salary) AS avg_salary FROM nurse_info GROUP BY shift;`                                       |
| 查看不同药品类别的药品数量与平均单价 | `SELECT category, COUNT(*) AS num_medicine, AVG(unit_price) AS avg_price FROM medicine_info GROUP BY category;` |

---

# 🔍 **3️⃣ 条件过滤查询（Filtering）**

📘 定义： WHERE / LIKE / BETWEEN / IN / IS NULL / NOT NULL 等过滤。

⚠️ 坑点方向
| 坑点         | 说明                       |
| ---------- | ------------------------ |
| NULL 误用    | `=` 与 `IS NULL` 混淆       |
| OR/AND 优先级 | 没加括号导致逻辑错                |
| 字符串匹配      | LIKE 区分大小写、通配符误用         |
| 枚举误解       | ENUM 值实际存储为字符串           |
| 日期比较       | DATE 类型 vs DATETIME 类型错误 |

| 自然语言问题             | 正确 SQL                                                         |
| ------------------ | -------------------------------------------------------------- |
| 查询工资高于20000元的医生姓名  | `SELECT name FROM doctor_info WHERE salary > 20000;`           |
| 查找性别为女且职称为“护师”的护士  | `SELECT name FROM nurse_info WHERE gender='女' AND title='护师';` |
| 查找仍在住院的病人（未出院）     | `SELECT name FROM patient_info WHERE discharge_date IS NULL;`  |
| 查询所有医院等级为“三甲”的医院名称 | `SELECT name FROM hospital_info WHERE level='三甲';`             |
| 找出所有药品库存少于1000的药品  | `SELECT name FROM medicine_info WHERE stock_quantity < 1000;`  |

---

# 📈 **4️⃣ 分组统计查询（Grouping）**

📘 定义： GROUP BY + 聚合函数（COUNT、SUM、AVG、MAX、MIN）。

⚠️ 坑点方向
| 坑点                | 说明                 |
| ----------------- | ------------------ |
| 非聚合列未在 GROUP BY 中 | MySQL 容忍但逻辑错误      |
| HAVING 与 WHERE 混用 | HAVING 应用于聚合结果     |
| GROUP BY NULL 值   | NULL 会被视为单组        |
| 多层嵌套              | GROUP BY 内嵌子查询聚合错误 |
| 维度歧义              | “每医院科室” vs “每医院”混淆 |


| 自然语言问题          | 正确 SQL                                                                                           |
| --------------- | ------------------------------------------------------------------------------------------------ |
| 各医院的床位总数        | `SELECT hospital_id, SUM(total_beds) AS total_beds_sum FROM hospital_info GROUP BY hospital_id;` |
| 各科室的平均床位数       | `SELECT dept_id, AVG(bed_count) AS avg_beds FROM department_info GROUP BY dept_id;`              |
| 每个医院的医生总人数      | `SELECT hospital_id, COUNT(*) AS doctor_count FROM doctor_info GROUP BY hospital_id;`            |
| 按医院等级统计医院数量     | `SELECT level, COUNT(*) FROM hospital_info GROUP BY level;`                                      |
| 每个班次的护士数量大于3的分组 | `SELECT shift, COUNT(*) AS num FROM nurse_info GROUP BY shift HAVING num > 3;`                   |

---

# 🏅 **5️⃣ 排名与排序查询（Ranking / Ordering）**
📘 定义： ORDER BY / LIMIT / RANK() / ROW_NUMBER() 等。

⚠️ 坑点方向

| 坑点           | 说明                      |
| ------------ | ----------------------- |
| ORDER BY 多字段 | 默认 ASC 导致优先级错误          |
| LIMIT 语义混乱   | LIMIT offset,count 顺序搞反 |
| 排名函数误用       | MySQL 8+ 才支持 RANK()     |
| 并列值处理        | RANK vs DENSE_RANK      |
| 子查询排序        | 外层排序覆盖内层结果              |



| 自然语言问题         | 正确 SQL                                                                                 |
| -------------- | -------------------------------------------------------------------------------------- |
| 工资最高的前5名医生是谁？  | `SELECT name, salary FROM doctor_info ORDER BY salary DESC LIMIT 5;`                   |
| 按入职日期最早的3位医生   | `SELECT name, hire_date FROM doctor_info ORDER BY hire_date ASC LIMIT 3;`              |
| 查询护士表中工资最高的2人  | `SELECT name, salary FROM nurse_info ORDER BY salary DESC LIMIT 2;`                    |
| 根据床位数从大到小排列医院  | `SELECT name, total_beds FROM hospital_info ORDER BY total_beds DESC;`                 |
| 按库存数量降序显示药品前5名 | `SELECT name, stock_quantity FROM medicine_info ORDER BY stock_quantity DESC LIMIT 5;` |

---

# ⏳ **6️⃣ 时间序列分析（Time Series）**

📘 定义： 按时间维度分析趋势、增长、环比等。

⚠️ 坑点方向

| 坑点                 | 说明                       |
| ------------------ | ------------------------ |
| DATE 与 DATETIME 混用 | 导致比较失败                   |
| 日期函数不统一            | MONTH() vs DATE_FORMAT() |
| 空日期                | NULL 时间被忽略               |
| 跨年聚合               | YEAR() 逻辑丢失              |
| 时区偏差               | TIMESTAMP 默认 UTC         |

| 自然语言问题            | 正确 SQL                                                                                                                                   |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| 统计2025年每个月的入院病人数  | `SELECT DATE_FORMAT(admission_date,'%Y-%m') AS month, COUNT(*) AS num FROM patient_info WHERE YEAR(admission_date)=2025 GROUP BY month;` |
| 查询2025年上半年入院的病人数量 | `SELECT COUNT(*) FROM patient_info WHERE admission_date BETWEEN '2025-01-01' AND '2025-06-30';`                                          |
| 按年份统计医院成立数量       | `SELECT YEAR(established_date) AS year, COUNT(*) AS num FROM hospital_info GROUP BY year;`                                               |
| 查询最近30天入院的病人姓名    | `SELECT name FROM patient_info WHERE admission_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);`                                            |
| 查询2024年维护过的设备数量   | `SELECT COUNT(*) FROM equipment_info WHERE YEAR(last_maintenance)=2024;`                                                                 |

---

# ⚖️ **7️⃣ 比较分析（Comparison）**
📘 定义： 比较不同维度或时间段的指标。

| 坑点      | 说明                  |
| ------- | ------------------- |
| 子查询返回多行 | 用 `=` 而非 `IN`       |
| 比较方向错误  | “高于平均值” 用 > AVG()   |
| 类型转换    | 字符串数值无法比较           |
| 条件重叠    | BETWEEN 边界包容性错误     |
| 日期差计算   | DATEDIFF() 返回天数，非秒数 |


| 自然语言问题           | 正确 SQL                                                                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| 查找工资高于平均工资的医生    | `SELECT name, salary FROM doctor_info WHERE salary > (SELECT AVG(salary) FROM doctor_info);`                                               |
| 查询库存高于平均库存的药品    | `SELECT name, stock_quantity FROM medicine_info WHERE stock_quantity > (SELECT AVG(stock_quantity) FROM medicine_info);`                   |
| 查找病人住院时间超过5天的记录  | `SELECT name, DATEDIFF(discharge_date, admission_date) AS stay_days FROM patient_info WHERE DATEDIFF(discharge_date, admission_date)>5;`   |
| 查询工资最低的医生与最高的医生  | `SELECT name, salary FROM doctor_info WHERE salary=(SELECT MIN(salary) FROM doctor_info) OR salary=(SELECT MAX(salary) FROM doctor_info);` |
| 查找成立时间早于2000年的医院 | `SELECT name, established_date FROM hospital_info WHERE established_date<'2000-01-01';`                                                    |

---

# 📉 **8️⃣ 占比分析（Proportion / Ratio）**
📘 定义： 计算某类占总量比例。

⚠️ 坑点方向

| 坑点     | 说明                          |
| ------ | --------------------------- |
| 除法整数截断 | COUNT(a)/COUNT(b) → 0       |
| 除数为0   | 需加 NULLIF() 避免报错            |
| 比例精度   | ROUND() 丢失小数位               |
| 多层子查询  | 内外层分母计算不一致                  |
| 同义混淆   | “比例”、“份额”、“占比” → 都需计算 ratio |

| 自然语言问题        | 正确 SQL                                                                                                                   |
| ------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 各职称医生占总人数比例   | `SELECT title, ROUND(COUNT(*)/(SELECT COUNT(*) FROM doctor_info)*100,2) AS percent FROM doctor_info GROUP BY title;`     |
| 各医院等级占比       | `SELECT level, ROUND(COUNT(*)/(SELECT COUNT(*) FROM hospital_info)*100,2) AS percent FROM hospital_info GROUP BY level;` |
| 按班次统计护士人数比例   | `SELECT shift, ROUND(COUNT(*)/(SELECT COUNT(*) FROM nurse_info)*100,2) AS percent FROM nurse_info GROUP BY shift;`       |
| 各药品类别在药品总数中占比 | `SELECT category, ROUND(COUNT(*)/(SELECT COUNT(*) FROM medicine_info)*100,2) FROM medicine_info GROUP BY category;`      |
| 不同性别病人比例      | `SELECT gender, ROUND(COUNT(*)/(SELECT COUNT(*) FROM patient_info)*100,2) FROM patient_info GROUP BY gender;`            |

---

# 🧾 **9️⃣ 汇总展示 / 报表结构查询（Summary / Report）**
📘 定义： 汇总型展示（按维度统计 + 汇总总计），如报表视图。

⚠️ 坑点方向

| 坑点             | 说明                  |
| -------------- | ------------------- |
| WITH ROLLUP 误解 | 产生 NULL 汇总行，易误认为空值  |
| 列别名错位          | 汇总结果与原字段名混用         |
| 多层汇总           | 两层 GROUP BY 嵌套丢失一致性 |
| 数据重复计数         | 汇总表与明细表重复           |
| 排序冲突           | 汇总列参与 ORDER BY 混乱   |



| 自然语言问题            | 正确 SQL                                                                                                |
| ----------------- | ----------------------------------------------------------------------------------------------------- |
| 显示各医院的床位总数并汇总总计   | `SELECT hospital_id, SUM(total_beds) AS bed_sum FROM hospital_info GROUP BY hospital_id WITH ROLLUP;` |
| 统计各职称的平均工资并显示总体平均 | `SELECT title, AVG(salary) AS avg_salary FROM doctor_info GROUP BY title WITH ROLLUP;`                |
| 汇总各科室的医生数量及总人数    | `SELECT dept_id, COUNT(*) AS num FROM doctor_info GROUP BY dept_id WITH ROLLUP;`                      |
| 汇总各医院等级的医院数及总计    | `SELECT level, COUNT(*) AS num FROM hospital_info GROUP BY level WITH ROLLUP;`                        |
| 汇总护士各班次的平均工资及总平均  | `SELECT shift, AVG(salary) FROM nurse_info GROUP BY shift WITH ROLLUP;`                               |

---

## 🧩 测试题集总结表

| 查询类型 | 数量 | 关键测试维度            |
| ---- | -- | ----------------- |
| 单指标  | 5  | COUNT、MAX、MIN 边界  |
| 多指标  | 5  | 聚合混合与别名           |
| 条件过滤 | 5  | NULL、AND/OR 优先级   |
| 分组统计 | 5  | GROUP BY + HAVING |
| 排名排序 | 5  | ORDER BY + LIMIT  |
| 时间序列 | 5  | DATE 函数           |
| 比较分析 | 5  | 子查询 + 比较          |
| 占比分析 | 5  | 除法精度、比例计算         |
| 汇总展示 | 5  | ROLLUP、汇总空行       |


