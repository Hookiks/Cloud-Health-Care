"""Référentiel RGPD : colonnes à caractère personnel (PII) à retirer
avant tout chargement dans l'entrepôt.

Le filtrage est centralisé ici pour être auditable : une seule source de
vérité décrit, par jeu de données, les champs directement identifiants
(nom, prénom, adresse, e-mail, téléphone, n° de sécurité sociale...).

Clé = nom logique de la source ; valeur = liste des colonnes à supprimer.
La comparaison est insensible à la casse (cf. ETL/common/rgpd.py).
"""

PII_FIELDS: dict[str, list[str]] = {
    # Table opérationnelle Patient -> DIM_PATIENT
    "patient": [
        "Nom",
        "Prenom",
        "Adresse",
        "EMail",
        "Tel",
        "Num_Secu",
    ],
    # Registre INSEE des décès -> FAIT_DECES (on ne garde que sexe / dates / géo)
    "deces": [
        "nom",
        "prenom",
        "numero_acte_deces",
    ],
    # Mutuelles -> DIM_MUTUELLE (l'adresse postale est retirée)
    "mutuelle": [
        "Adresse",
    ],
    # Professionnels de santé -> DIM_PROFESSIONNEL
    # (nom/prénom retirés : on conserve l'identifiant pro et la spécialité)
    "professionnel": [
        "Nom",
        "Prenom",
    ],
}
