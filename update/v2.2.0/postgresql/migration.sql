
-- pidrelations_pidrelation
-- 既存のプライマリキー削除
ALTER TABLE pidrelations_pidrelation DROP CONSTRAINT pk_pidrelations_pidrelation;
-- 新しいプライマリキーを設定
ALTER TABLE pidrelations_pidrelation ADD CONSTRAINT pk_pidrelations_pidrelation PRIMARY KEY (parent_id, child_id, relation_type);
