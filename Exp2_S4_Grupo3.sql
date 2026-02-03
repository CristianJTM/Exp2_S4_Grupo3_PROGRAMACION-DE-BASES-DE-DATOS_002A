/*-------------------CASO 1---------------------*/

ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY';
SET SERVEROUTPUT ON;

-- Variables bind para parametrizar los tramos de monto anual
VARIABLE b_tramo_1_inf NUMBER;
EXEC :b_tramo_1_inf := 500000;

VARIABLE b_tramo_1_sup NUMBER;
EXEC :b_tramo_1_sup := 700000;

VARIABLE b_tramo_2_inf NUMBER;
EXEC :b_tramo_2_inf := 700001;

VARIABLE b_tramo_2_sup NUMBER;
EXEC :b_tramo_2_sup := 900000;

-- Variable bind para el año del proceso
VARIABLE b_agno_proceso NUMBER;
EXEC :b_agno_proceso := EXTRACT(YEAR FROM SYSDATE) - 1;

-- Variables bind para los códigos de transacción
--Compras Tiendas Retail o Asociadas
VARIABLE b_cod_compra NUMBER;
EXEC :b_cod_compra := 101;

--Avance en Efectivo
VARIABLE b_cod_avance NUMBER;
EXEC :b_cod_avance := 102;

--S�per Avance en Efectivo
VARIABLE b_cod_superavance NUMBER;
EXEC :b_cod_superavance := 103;

DECLARE
    -- Varray con los puntos según tramo
    TYPE t_puntos IS VARRAY(4) OF NUMBER;
    v_puntos t_puntos := t_puntos(
        250,  -- puntos base por cada 100.000
        300,  -- puntos extra tramo 1
        550,  -- puntos extra tramo 2
        700   -- puntos extra tramo superior
    );

    -- Cursor variable para recorrer todas las transacciones del año
    v_cur_detalle SYS_REFCURSOR;
    reg_detalle detalle_puntos_tarjeta_catb%ROWTYPE;

    -- Variables auxiliares
    v_puntos_base NUMBER;
    v_puntos_extra NUMBER;
    v_cod_tipo_cliente cliente.cod_tipo_cliente%TYPE;
    v_cod_tptran tipo_transaccion_tarjeta.cod_tptran_tarjeta%TYPE;

    -- Cursor explícito con parámetro para resumen mensual
    CURSOR cur_resumen_mes(p_mes_anno VARCHAR2) IS
        SELECT
            SUM(CASE WHEN cod_trans = :b_cod_compra THEN monto_transaccion ELSE 0 END) AS monto_total_compras,
            SUM(CASE WHEN cod_trans = :b_cod_compra THEN puntos_allthebest ELSE 0 END) AS total_puntos_compras,
            SUM(CASE WHEN cod_trans = :b_cod_avance THEN monto_transaccion ELSE 0 END) AS monto_total_avances,
            SUM(CASE WHEN cod_trans = :b_cod_avance THEN puntos_allthebest ELSE 0 END) AS total_puntos_avances,
            SUM(CASE WHEN cod_trans = :b_cod_superavance THEN monto_transaccion ELSE 0 END) AS monto_total_savances,
            SUM(CASE WHEN cod_trans = :b_cod_superavance THEN puntos_allthebest ELSE 0 END) AS total_puntos_savances
        FROM (
            -- Asignamos códigos de transacción de manera virtual para el resumen usando variables bind
            SELECT
                monto_transaccion,
                puntos_allthebest,
                CASE
                    WHEN tipo_transaccion LIKE '%Compras%' THEN :b_cod_compra
                    WHEN tipo_transaccion = 'Avance en Efectivo' THEN :b_cod_avance
                    WHEN tipo_transaccion LIKE '%Avance%' THEN :b_cod_superavance
                    ELSE NULL
                END AS cod_trans
            FROM detalle_puntos_tarjeta_catb
            WHERE TO_CHAR(fecha_transaccion,'MMYYYY') = p_mes_anno
        );

    reg_resumen resumen_puntos_tarjeta_catb%ROWTYPE;

BEGIN
    -- Limpiamos las tablas antes de ejecutar el proceso para que sea repetible
    EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_puntos_tarjeta_catb';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE resumen_puntos_tarjeta_catb';

    -- Abrimos cursor para recorrer todas las transacciones del año
    OPEN v_cur_detalle FOR
        SELECT
            c.numrun,
            c.dvrun,
            tc.nro_tarjeta,
            ttc.nro_transaccion,
            ttc.fecha_transaccion,
            tpt.nombre_tptran_tarjeta,
            ttc.monto_transaccion,
            c.cod_tipo_cliente,
            tpt.cod_tptran_tarjeta
        FROM cliente c
        JOIN tarjeta_cliente tc ON c.numrun = tc.numrun
        JOIN transaccion_tarjeta_cliente ttc ON tc.nro_tarjeta = ttc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta tpt ON ttc.cod_tptran_tarjeta = tpt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_agno_proceso
        ORDER BY ttc.fecha_transaccion, c.numrun, ttc.nro_transaccion;

    -- Loop para calcular los puntos y llenar la tabla de detalle
    LOOP
        FETCH v_cur_detalle INTO
            reg_detalle.numrun,
            reg_detalle.dvrun,
            reg_detalle.nro_tarjeta,
            reg_detalle.nro_transaccion,
            reg_detalle.fecha_transaccion,
            reg_detalle.tipo_transaccion,
            reg_detalle.monto_transaccion,
            v_cod_tipo_cliente,
            v_cod_tptran;

        EXIT WHEN v_cur_detalle%NOTFOUND;

        -- Calculamos los puntos base
        v_puntos_base := FLOOR(reg_detalle.monto_transaccion / 100000) * v_puntos(1);

        -- Calculamos puntos extra solo para dueños de casa y pensionados
        v_puntos_extra := 0;
        IF v_cod_tipo_cliente IN (30,40) THEN
            IF reg_detalle.monto_transaccion BETWEEN :b_tramo_1_inf AND :b_tramo_1_sup THEN
                v_puntos_extra := FLOOR(reg_detalle.monto_transaccion / 100000) * v_puntos(2);
            ELSIF reg_detalle.monto_transaccion BETWEEN :b_tramo_2_inf AND :b_tramo_2_sup THEN
                v_puntos_extra := FLOOR(reg_detalle.monto_transaccion / 100000) * v_puntos(3);
            ELSIF reg_detalle.monto_transaccion > :b_tramo_2_sup THEN
                v_puntos_extra := FLOOR(reg_detalle.monto_transaccion / 100000) * v_puntos(4);
            END IF;
        END IF;

        reg_detalle.puntos_allthebest := v_puntos_base + v_puntos_extra;

        -- Insertamos en la tabla de detalle
        INSERT INTO detalle_puntos_tarjeta_catb(
            numrun, dvrun, nro_tarjeta, nro_transaccion, 
            fecha_transaccion, tipo_transaccion, monto_transaccion, puntos_allthebest
        ) VALUES (
            reg_detalle.numrun, reg_detalle.dvrun, reg_detalle.nro_tarjeta, reg_detalle.nro_transaccion,
            reg_detalle.fecha_transaccion, reg_detalle.tipo_transaccion, reg_detalle.monto_transaccion, 
            reg_detalle.puntos_allthebest
        );

    END LOOP;
    CLOSE v_cur_detalle;

    -- Loop para generar el resumen mensual
    FOR r_mes IN (
        SELECT DISTINCT TO_CHAR(fecha_transaccion,'MMYYYY') AS mes_anno
        FROM detalle_puntos_tarjeta_catb
        ORDER BY mes_anno
    ) LOOP
        OPEN cur_resumen_mes(r_mes.mes_anno);
        FETCH cur_resumen_mes INTO
            reg_resumen.monto_total_compras,
            reg_resumen.total_puntos_compras,
            reg_resumen.monto_total_avances,
            reg_resumen.total_puntos_avances,
            reg_resumen.monto_total_savances,
            reg_resumen.total_puntos_savances;
        CLOSE cur_resumen_mes;

        reg_resumen.mes_anno := r_mes.mes_anno;

        -- Insertamos en la tabla de resumen
        INSERT INTO resumen_puntos_tarjeta_catb
        VALUES reg_resumen;
    END LOOP;

    -- Confirmamos los cambios
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Caso 1 ejecutado correctamente.');

EXCEPTION
    WHEN OTHERS THEN
        -- En caso de error, deshacemos cambios
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error en Caso 1: ' || SQLERRM);
END;
/


SELECT * FROM detalle_puntos_tarjeta_catb;

SELECT * FROM resumen_puntos_tarjeta_catb;
/*-------------------CASO 2---------------------*/

-- Bloque PL/SQL para calcular los aportes a SBIF por avances y súper avances
-- Totalmente parametrizable y con manejo de errores

ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY';
SET SERVEROUTPUT ON;

-- Variable bind para el año de ejecución (correspondiente al año de las transacciones)
VARIABLE b_agno_proceso NUMBER;
EXEC :b_agno_proceso := EXTRACT(YEAR FROM SYSDATE);

-- Variables bind para códigos de transacción
--Avance en Efectivo
VARIABLE b_cod_avance NUMBER;
EXEC :b_cod_avance := 102;

--S�per Avance en Efectivo
VARIABLE b_cod_superavance NUMBER;
EXEC :b_cod_superavance := 103;

DECLARE
    -- Cursores explícitos

    -- Cursor detalle: obtiene todas las transacciones del tipo avance o súper avance
    CURSOR cur_detalle IS
        SELECT 
            c.numrun,
            c.dvrun,
            tc.nro_tarjeta,
            ttc.nro_transaccion,
            ttc.fecha_transaccion,
            ttt.nombre_tptran_tarjeta,
            ttc.monto_total_transaccion,
            tas.porc_aporte_sbif
        FROM cliente c
        JOIN tarjeta_cliente tc
            ON tc.numrun = c.numrun
        JOIN transaccion_tarjeta_cliente ttc
            ON tc.nro_tarjeta = ttc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta ttt
            ON ttt.cod_tptran_tarjeta = ttc.cod_tptran_tarjeta
        JOIN tramo_aporte_sbif tas
            ON ttc.monto_total_transaccion BETWEEN tas.tramo_inf_av_sav AND tas.tramo_sup_av_sav
        WHERE ttc.cod_tptran_tarjeta IN (:b_cod_avance, :b_cod_superavance)
          AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_agno_proceso
        ORDER BY EXTRACT(YEAR FROM ttc.fecha_transaccion) ASC,
                 EXTRACT(MONTH FROM ttc.fecha_transaccion) ASC,
                 ttc.cod_tptran_tarjeta ASC,
                 ttc.fecha_transaccion ASC,
                 c.numrun ASC;

    -- Cursor resumen mensual por tipo de transacción
    CURSOR cur_resumen(p_mes_anno VARCHAR2) IS
        SELECT 
            tipo_transaccion,
            SUM(monto_transaccion) AS monto_total_transacciones,
            SUM(aporte_sbif) AS aporte_total_abif
        FROM detalle_aporte_sbif
        WHERE TO_CHAR(fecha_transaccion,'MMYYYY') = p_mes_anno
        GROUP BY tipo_transaccion
        ORDER BY tipo_transaccion;

    -- Registros para hacer fetch de los cursores
    reg_detalle DETALLE_APORTE_SBIF%ROWTYPE;
    reg_resumen RESUMEN_APORTE_SBIF%ROWTYPE;

BEGIN
    -- Limpiamos las tablas antes de llenar los datos
    EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_aporte_sbif';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE resumen_aporte_sbif';

    -- Procesamiento de detalle de avances y súper avances
    FOR r_det IN cur_detalle LOOP
        -- Copiamos los datos del cursor al registro
        reg_detalle.numrun := r_det.numrun;
        reg_detalle.dvrun := r_det.dvrun;
        reg_detalle.nro_tarjeta := r_det.nro_tarjeta;
        reg_detalle.nro_transaccion := r_det.nro_transaccion;
        reg_detalle.fecha_transaccion := r_det.fecha_transaccion;
        reg_detalle.tipo_transaccion := r_det.nombre_tptran_tarjeta;
        reg_detalle.monto_transaccion := r_det.monto_total_transaccion;

        -- Calculamos el aporte SBIF usando PL/SQL
        reg_detalle.aporte_sbif := ROUND(r_det.monto_total_transaccion * r_det.porc_aporte_sbif / 100);

        -- Insertamos en la tabla de detalle
        INSERT INTO detalle_aporte_sbif
        VALUES reg_detalle;
    END LOOP;

    -- Procesamiento de resumen mensual
    FOR r_mes IN (
        SELECT DISTINCT TO_CHAR(fecha_transaccion,'MMYYYY') AS mes_anno
        FROM detalle_aporte_sbif
        ORDER BY mes_anno
    ) LOOP
        -- Abrimos cursor de resumen mensual por mes y tipo de transacción
        OPEN cur_resumen(r_mes.mes_anno);
        LOOP
            FETCH cur_resumen INTO
                reg_resumen.tipo_transaccion,
                reg_resumen.monto_total_transacciones,
                reg_resumen.aporte_total_abif;
            EXIT WHEN cur_resumen%NOTFOUND;

            reg_resumen.mes_anno := r_mes.mes_anno;

            -- Insertamos en la tabla de resumen
            INSERT INTO resumen_aporte_sbif
            VALUES reg_resumen;
        END LOOP;
        CLOSE cur_resumen;
    END LOOP;

    -- Confirmamos los cambios
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Caso 2 ejecutado correctamente.');
    DBMS_OUTPUT.PUT_LINE('Proceso de aportes SBIF ejecutado correctamente.');

EXCEPTION
    WHEN OTHERS THEN
        -- En caso de error, deshacemos cambios y mostramos mensaje
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error en Caso 2');
        DBMS_OUTPUT.PUT_LINE('Error en el proceso de aportes SBIF: ' || SQLERRM);

END;
/

SELECT * FROM detalle_aporte_sbif;
SELECT * FROM resumen_aporte_sbif;