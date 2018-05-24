CREATE TABLE finished_url(id SERIAL, url VARCHAR UNIQUE, type VARCHAR);
CREATE TABLE baike_html(id SERIAL, html BYTEA, url VARCHAR UNIQUE, type VARCHAR);
CREATE TABLE baike_knowledge(id SERIAL, url VARCHAR UNIQUE, knowledge JSON, type VARCHAR);
CREATE TABLE url_label(id SERIAL, url VARCHAR UNIQUE, label JSON, type VARCHAR);
CREATE TABLE manual_label(id SERIAL,
    url VARCHAR UNIQUE,
    count INT,
    ratio FLOAT,
    label JSON,
    flag VARCHAR,
    CHECK (flag IN ('auto', 'human')),
    knowledge JSON,
    type VARCHAR);

CREATE TABLE entity_count(
    id SERIAL,
    keyword VARCHAR UNIQUE,
    count INT
)
CREATE TABLE mention2entity(id SERIAL, mention VARCHAR, title VARCHAR, url VARCHAR, UNIQUE(mention, title, url))



# rule


update manual_label set label = '{"entity_type":["LOCATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}街道办事处(\/|$)'

update manual_label set label = '{"entity_type":["LOCATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}(火|汽)车站(\/|$)'

update manual_label set label = '{"entity_type":["LOCATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}国家地质公园(\/|$)'
update manual_label set label = '{"entity_type":["LOCATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}工业园区(\/|$)'

update manual_label set label = '{"entity_type":["ORGANIZATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}自然科学基金(\/|$)'
update manual_label set label = '{"entity_type":["ORGANIZATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}出版社(\/|$)'
update manual_label set label = '{"entity_type":["ORGANIZATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}爱乐乐团(\/|$)'

update manual_label set label = '{"entity_type":["ORGANIZATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}民政局(\/|$)'
update manual_label set label = '{"entity_type":["ORGANIZATION"]}'
    where url ~ 'https://baike.baidu.com/item/.{2,}人民政府(\/|$)'

# 普通词


# 事件
update manual_label set label = '{"entity_type":["Event"]}', flag = 'manual'
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['发生时间', '时间', '持续时间', '结束时间', '发生地']
      and (knowledge->'attrs'->'open_tags')::jsonb ?| array['事件', '历史事件', '外国历史事件', '古代历史事件', '现代历史事件']
      and flag != 'manual'

# 时间
update manual_label set label = '{"entity_type":["Time"]}', flag = 'manual'
    where url ~ 'https://baike.baidu.com/item/(公元前|公元)?[1-9一二三四五六七八九][0-9一二三四五六七八九]+年([1-9一二三四五六七八九][0-9一二三四五六七八九]?月([1-9一二三四五六七八九][0-9一二三四五六七八九]?日)?)?(\/|$)'


# 族群 (人种，民族，地域，文化，姓氏等多角度)
update manual_label set label = '{"entity_type":["Ethnic_Group"]}', flag = 'manual'
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['分布', '信仰', '分布地区', '人数', '语言', '始祖']
      and not (knowledge->'attrs'->'infobox')::jsonb ?| array['出生地', '民族', '出生日期', '使用人口']
      and (knowledge->'attrs'->'open_tags')::jsonb ?| array['民族', '人种']
      and not (knowledge->'attrs'->'open_tags')::jsonb ?| array['人物', '文化', '历史', '建筑', '地理', '地点',  '期刊', '小说', '出版物', '社会', '宗教', '语言', '组织机构', '小学']
      and flag != 'manual'

# 语言
update manual_label set label = '{"entity_type":["Language"]}'
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['语系']
      and (knowledge->'attrs'->'open_tags')::jsonb ?| array['语言']
      and not (knowledge->'attrs'->'open_tags')::jsonb ?| array['字词']
      and flag != 'manual'

# 货币
update manual_label set label = '{"entity_type":["Currency"]}'
    where (knowledge->'attrs'->'infobox')::jsonb ?| array['货币代码', '货币类型']
      and flag != 'manual'



# 统计各类型数量
select *, count(*) from
	(select json_array_elements_text(label->'entity_type') as type from manual_label) as types
	group by types.type