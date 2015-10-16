/*
Création d ela vue de mise au format des données
*/

CREATE OR REPLACE VIEW odk.obs_mobile_demo_aten AS 
 WITH tableau_de_sommets AS ( 
		/* La chaine contenant le "geohape" est transformée en tableau de sommets*/
         SELECT loc."_URI", 
            unnest(string_to_array(btrim(COALESCE(loc."SHAPE_TEXT", pg_catalog.concat(loc."GPS_LAT", ' ', loc."GPS_LNG", ' 0.0 0.0')::character varying)::text, ';'::text), ';'::text)) AS elet, 
            string_to_array(btrim(COALESCE(loc."SHAPE_TEXT", pg_catalog.concat(loc."GPS_LAT", ' ', loc."GPS_LNG", ' 0.0 0.0')::character varying)::text, ';'::text), ';'::text) AS tab_orig, 
            array_length(string_to_array(btrim(COALESCE(loc."SHAPE_TEXT", pg_catalog.concat(loc."GPS_LAT", ' ', loc."GPS_LNG", ' 0.0 0.0')::character varying)::text, ';'::text), ';'::text), 1) AS nb_noeuds, 
            generate_series(1, array_length(string_to_array(btrim(COALESCE(loc."SHAPE_TEXT", pg_catalog.concat(loc."GPS_LAT", ' ', loc."GPS_LNG", ' 0.0 0.0')::character varying)::text, ';'::text), ';'::text), 1), 1) AS rang
           FROM odk."DEMO_ATEN_NOUVELLE_LOCALITE" loc
        ), geom AS ( 
		/* ces tableaux de points sont transformés en géomatrie : 
		-> points si un seul élément dans le tableau, 
		-> une ligne si plusieurs éléments dans le tableau */
         SELECT tableau_de_sommets."_URI", 
            st_transform(
                CASE
                    WHEN tableau_de_sommets.nb_noeuds = 1 THEN st_union(st_setsrid(st_makepoint(split_part(tableau_de_sommets.elet, ' '::text, 2)::numeric::double precision, split_part(tableau_de_sommets.elet, ' '::text, 1)::numeric::double precision), 4326))
                    WHEN tableau_de_sommets.nb_noeuds > 1 AND st_isclosed(st_makeline(array_agg(st_setsrid(st_makepoint(split_part(tableau_de_sommets.elet, ' '::text, 2)::numeric::double precision, split_part(tableau_de_sommets.elet, ' '::text, 1)::numeric::double precision), 4326) ORDER BY tableau_de_sommets.rang))) THEN st_makepolygon(st_makeline(array_agg(st_setsrid(st_makepoint(split_part(tableau_de_sommets.elet, ' '::text, 2)::numeric::double precision, split_part(tableau_de_sommets.elet, ' '::text, 1)::numeric::double precision), 4326) ORDER BY tableau_de_sommets.rang)))
                    ELSE st_makeline(array_agg(st_setsrid(st_makepoint(split_part(tableau_de_sommets.elet, ' '::text, 2)::numeric::double precision, split_part(tableau_de_sommets.elet, ' '::text, 1)::numeric::double precision), 4326) ORDER BY tableau_de_sommets.rang))
                END, 2154) AS geometrie
           FROM tableau_de_sommets
          GROUP BY tableau_de_sommets."_URI", tableau_de_sommets.nb_noeuds
        )
 /* on reconstitue chacune des données par jointure entyre les tables CORE, LOCALITE, OBSERVATION, et on "ajoute" la géometrie calculée précédemment */
 SELECT core."_URI", core."RELEVE_DATE_OBS"::date AS date_obs, taxref.regne, 
    taxref.nom_complet, 
    btrim(split_part(obs."LB_CD_NOM_LATIN"::text, '{'::text, 2), '}'::text) AS cd_nom, 
        CASE
            WHEN taxref.regne::text = 'Animalia'::text THEN obs."CARAC_OBSERVATION_FAUNE_FAUNE_EFFECTIF"
            ELSE obs."CARAC_OBSERVATION_FLORE_FLORE_EFFECTIF"
        END AS effectif, 
    obs."CARAC_OBSERVATION_FLORE_EFFECTIF_TEXTUEL" AS effectif_text, 
    obs."CARAC_OBSERVATION_FAUNE_TYPE_EFFECTIF" AS type_effectif, 
    obs."CARAC_OBSERVATION_FLORE_PHENOLOGIE" AS phenologie, 
    obs."_URI" AS id_waypoint, 
    COALESCE(localite."GPS_LNG"::double precision, st_x(st_transform(st_centroid(geom.geometrie), 4326))) AS long, 
    COALESCE(localite."GPS_LAT"::double precision, st_y(st_transform(st_centroid(geom.geometrie), 4326))) AS lat, 
        CASE
            WHEN taxref.regne::text = 'Animalia'::text THEN obs."CARAC_OBSERVATION_FAUNE_LOCAL_RQS"
            WHEN taxref.regne::text = 'Plantae'::text THEN obs."CARAC_OBSERVATION_FLORE_LOCAL_RQS"
            ELSE NULL::character varying
        END AS remarque_localisation, 
    btrim(split_part(core."RELEVE_CODE_OBSERVATEUR"::text, '{'::text, 2), '}'::text) AS observateurs, 
    btrim(split_part(core."RELEVE_CODE_OBSERVATEUR"::text, '{'::text, 2), '}'::text)::integer AS numerisateur, 
    '2'::text AS structure, 
    pg_catalog.concat('id sicen_mobil ', obs."_URI", 
        CASE
            WHEN localite."ORIGINE_LOCALISATION"::text = 'gps'::text THEN pg_catalog.concat('. Précision GPS : ', localite."GPS_ACC", '. ')
            ELSE NULL::text
        END, 
        CASE
            WHEN taxref.regne::text = 'Animalia'::text THEN obs."CARAC_OBSERVATION_FAUNE_OBS_RQS"
            WHEN taxref.regne::text = 'Plantae'::text THEN obs."CARAC_OBSERVATION_FLORE_OBS_RQS"
            ELSE NULL::character varying
        END) AS remarque_obs, 
    commune.insee_com, 'GPS'::saisie.enum_precision AS "precision", 
    'à valider'::saisie.enum_statut_validation AS statut_validation, 
    btrim(split_part(core."RELEVE_ID_ETUDE"::text, '{'::text, 2), '}'::text)::integer AS id_etude, 
    btrim(split_part(core."RELEVE_ID_PROTOCOLE"::text, '{'::text, 2), '}'::text)::integer AS id_protocole, 
    geom.geometrie::geometry(Geometry,2154) AS geometrie
   FROM odk."DEMO_ATEN_CORE" core
   JOIN odk."DEMO_ATEN_NOUVELLE_LOCALITE" localite ON localite."_PARENT_AURI"::text = core."_URI"::text
   JOIN odk."DEMO_ATEN_NOUVELLE_OBSERVATION" obs ON localite."_URI"::text = obs."_PARENT_AURI"::text
   JOIN geom ON geom."_URI"::text = localite."_URI"::text
   JOIN inpn.taxref ON taxref.cd_nom::text = btrim(split_part(obs."LB_CD_NOM_LATIN"::text, '{'::text, 2), '}'::text)
   JOIN ign_geofla.commune ON st_within(st_centroid(geom.geometrie), commune.geometrie)
   LEFT JOIN saisie.suivi_saisie_observation ON suivi_saisie_observation.id_waypoint = obs."_URI"::text
  WHERE suivi_saisie_observation.id_waypoint IS NULL
  ORDER BY localite."_ORDINAL_NUMBER", obs."_ORDINAL_NUMBER";


/*
Création de la fonction appellée par le trigger
*/

CREATE OR REPLACE FUNCTION odk.alimente_saisie_observation_demo_aten()
  RETURNS trigger AS
$BODY$ declare
BEGIN
INSERT INTO saisie.saisie_observation(
            date_obs, regne, nom_complet, cd_nom, effectif, effectif_textuel, type_effectif, phenologie, id_waypoint, 
            longitude, latitude, localisation, observateur, numerisateur, 
            structure, remarque_obs, code_insee, 
            "precision", statut_validation, id_etude, id_protocole, 
            geometrie)
SELECT date_obs, regne, nom_complet, cd_nom, effectif, effectif_text, 
       type_effectif, phenologie, id_waypoint, long, lat, remarque_localisation, observateurs, 
       numerisateur, structure, remarque_obs, insee_com, "precision", 
       statut_validation, id_etude, id_protocole, geometrie
  FROM odk.obs_mobile_demo_aten
 
  WHERE obs_mobile_demo_aten."_URI"  = NEW."_URI";
RETURN NULL; 
END; $BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION odk.alimente_saisie_observation_demo_aten()
  OWNER TO dba;

/* 
Création du trigger : 
aprés chaque insertion dans la table "CORE", insère la données correspondante 
(calculée par la vue) dans la table saisie. saisie_observation 
*/

CREATE TRIGGER demo_aten_alimente_saisie_obs
AFTER INSERT
ON odk."DEMO_ATEN_CORE"
FOR EACH ROW
EXECUTE PROCEDURE odk.alimente_saisie_observation_demo_aten();
