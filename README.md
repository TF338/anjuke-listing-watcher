# 安居客房源监控工具

[English README](./README_en.md)

## 功能特点

- 支持多种房源类型：租房、买房
- 价格区间筛选
- 面积区间筛选
- 关键词过滤
- 持久化缓存（避免重复抓取）
- 文件或邮件通知
- 完整的日志记录

## 安装

```bash
pip install requests beautifulsoup4 pyyaml
```

## 快速开始

1. 复制配置示例文件
```bash
cp config.example.yaml config.yaml
```

2. 编辑 `config.yaml` 配置参数

3. 运行程序
```bash
python3 anjuke_scraper.py
```

## 配置说明

| 参数 | 说明 | 示例 |
|------|------|------|
| city | 城市代码（2位拼音） | km, sz, sh |
| listing_type | 房源类型 | rent_apartment, sale_apartment |
| price_min | 最低价格 | 1000 |
| price_max | 最高价格 | 3000 |
| area_min | 最小面积（平米） | 40 |
| area_max | 最大面积（平米） | 100 |
| keywords | 关键词列表 | ["地铁", "精装"] |
| output_mode | 输出模式 | file / email |

## 运行程序

```bash
# 默认运行
python3 anjuke_scraper.py

# 指定配置文件
python3 anjuke_scraper.py --config /path/to/config.yaml
```

## 运行测试

```bash
# 运行所有测试
pytest

# 仅运行集成测试
pytest -m integration

# 排除慢速测试
pytest -m "not slow"
```

## 项目结构

```
.
├── anjuke_scraper.py    # 主程序
├── crawler.py           # 爬虫模块
├── config.yaml         # 配置文件
├── config.example.yaml # 配置示例
├── cache.db           # 缓存数据库（自动创建）
├── listings.txt        # 输出结果
├── tests/             # 测试目录
└── pytest.ini         # pytest配置
```

## 城市代码

城市代码取自安居客 URL，例如：
- 昆明: km
- 深圳: sz  
- 上海: sh
- 北京: bj
- 广州: gz

完整列表请访问 https://www.anjuke.com
