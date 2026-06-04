-- =====================================================================
--  CHU Data Warehouse — Index (accélération des jointures / filtres)
-- =====================================================================

-- Clés étrangères des faits (jointures avec les dimensions)
CREATE INDEX IF NOT EXISTS ix_fc_patient        ON "FAIT_CONSULTATION"(patient_key);
CREATE INDEX IF NOT EXISTS ix_fc_prof           ON "FAIT_CONSULTATION"(professionnel_key);
CREATE INDEX IF NOT EXISTS ix_fc_etab           ON "FAIT_CONSULTATION"(etablissement_key);
CREATE INDEX IF NOT EXISTS ix_fc_diag           ON "FAIT_CONSULTATION"(diagnostic_key);
CREATE INDEX IF NOT EXISTS ix_fc_date           ON "FAIT_CONSULTATION"(date_key);
CREATE INDEX IF NOT EXISTS ix_fc_annee          ON "FAIT_CONSULTATION"(annee);

CREATE INDEX IF NOT EXISTS ix_fh_patient        ON "FAIT_HOSPITALISATION"(patient_key);
CREATE INDEX IF NOT EXISTS ix_fh_etab           ON "FAIT_HOSPITALISATION"(etablissement_key);
CREATE INDEX IF NOT EXISTS ix_fh_diag           ON "FAIT_HOSPITALISATION"(diagnostic_key);
CREATE INDEX IF NOT EXISTS ix_fh_date           ON "FAIT_HOSPITALISATION"(date_key);
CREATE INDEX IF NOT EXISTS ix_fh_annee          ON "FAIT_HOSPITALISATION"(annee);

CREATE INDEX IF NOT EXISTS ix_fd_loc            ON "FAIT_DECES"(localisation_key);
CREATE INDEX IF NOT EXISTS ix_fd_annee          ON "FAIT_DECES"(annee);

CREATE INDEX IF NOT EXISTS ix_fs_loc            ON "FAIT_SATISFACTION"(localisation_key);
CREATE INDEX IF NOT EXISTS ix_fs_annee          ON "FAIT_SATISFACTION"(annee);

-- Filtres fréquents sur les dimensions
CREATE INDEX IF NOT EXISTS ix_dp_sexe           ON "DIM_PATIENT"(sexe);
CREATE INDEX IF NOT EXISTS ix_dp_tranche        ON "DIM_PATIENT"(tranche_age);
CREATE INDEX IF NOT EXISTS ix_de_region         ON "DIM_ETABLISSEMENT"(region);
CREATE INDEX IF NOT EXISTS ix_dt_annee          ON "DIM_TEMPS"(annee);
