
CREATE TABLE youdao_bilingual(id SERIAL, keyword VARCHAR, direction VARCHAR, data VARCHAR);

CREATE TABLE iciba(id SERIAL, keyword VARCHAR, direction VARCHAR, data VARCHAR);

CREATE TABLE bing(id SERIAL, keyword VARCHAR);

CREATE TABLE baidu(id SERIAL, keyword VARCHAR);

CREATE TABLE dictall(id SERIAL, category VARCHAR, url VARCHAR, data VARCHAR);
CREATE TABLE dictall_term(id SERIAL, ch VARCHAR, en VARCHAR, url VARCHAR, category VARCHAR, UNIQUE(ch, en, url, category));

CREATE TABLE ch2en(id SERIAL, ch VARCHAR, en VARCHAR, url VARCHAR, source VARCHAR, UNIQUE(ch, en, source, url));

CREATE TABLE raw_terms(id SERIAL, domain VARCHAR, name VARCHAR, data VARCHAR, source VARCHAR)

CREATE TABLE raw_html(id SERIAL, url VARCHAR, html VARCHAR, type VARCHAR)



create temporary table uniq_ids as select min(b.id) from ch2en b group by b.ch, b.en, b.url, b.source;
DELETE FROM ch2en a where not exists(select * from uniq_ids where a.id = uniq_ids.min);