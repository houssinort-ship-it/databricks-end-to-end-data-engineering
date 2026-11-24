SELECT date_commande, mois, statut, nom_client, ville_client,
       categorie, designation, quantite, montant_ligne
FROM workspace.gold.gold_fact_ventes
WHERE coalesce(statut, '') NOT LIKE 'annul%';
