-- 00: RESET volatile tables (Teradata-direct)
-- QUESTION: none. Housekeeping only.
-- WHEN TO RUN: only when rerunning a pack in the SAME session and it fails with
--   "table already exists". On a fresh session do NOT run this: every line errors
--   "does not exist", which is harmless but noisy.
-- WHAT TO DO WITH IT: run, ignore all output, then run the pack.
-- Packs never contain DROP statements (2026-09-04: DROP errors made success unreadable).
-- =============================================================================
DROP TABLE vt_em_decis_cards;
DROP TABLE vt_tactic_ids;
DROP TABLE vt_master_cards;
DROP TABLE vt_sent_cards;
DROP TABLE vt_em_decis57;
DROP TABLE vt_tactic_ids57;
DROP TABLE vt_master57;
DROP TABLE vt_sent_evt57;
DROP TABLE vt_sent57;
DROP TABLE vt_first_unsub57;
DROP TABLE vt_zero_send_months57;
DROP TABLE vt_em_decis58;
DROP TABLE vt_tactic_ids58;
DROP TABLE vt_master58;
DROP TABLE vt_sent_evt58;
DROP TABLE vt_sent58;
DROP TABLE vt_first_unsub_by_mne58;
DROP TABLE vt_zero_send_months58;
DROP TABLE vt_em_decis59;
DROP TABLE vt_tactic_ids59;
DROP TABLE vt_master59;
DROP TABLE vt_sent_evt59;
DROP TABLE vt_sent59;
DROP TABLE vt_first_unsub_by_mne59;
DROP TABLE vt_zero_send_months59;
DROP TABLE vt_same_mne59;
DROP TABLE vt_cpc_clients59;
DROP TABLE vt_cpc_log59;
DROP TABLE vt_cpc_asof_latest59;
DROP TABLE vt_cpc_state_asof59;
DROP TABLE vt_sf_unsub60;
DROP TABLE vt_cpc_write60;
DROP TABLE vt_pairs60;
DROP TABLE vt_sf_flagged60;
DROP TABLE vt_cpc_flagged60;
DROP TABLE vt_1012_email61;
DROP TABLE vt_1046_61;
DROP TABLE vt_sf_unsub62;
DROP TABLE vt_sf_clients62;
DROP TABLE vt_cpc_write62;
DROP TABLE vt_classified62;
DROP TABLE vt_sf_unsub64;
DROP TABLE vt_cpc_write64;
DROP TABLE vt_pairs64;
DROP TABLE vt_cpc_flagged64;
DROP TABLE vt_cpc_only_7020_64;
DROP TABLE vt_cpc_only_clients64;
DROP TABLE vt_master_keys64;
DROP TABLE vt_event_sent64;
DROP TABLE vt_event_unsub64;
DROP TABLE vt_cpc_only_master64;
