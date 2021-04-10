/***********************************************************
ELABORADO POR:   Juan Gabriel Catuche Girón
EMPRESA:         MVM Ingeniería de Software
FECHA:           Marzo 2021
WOEPM:           WO958309

Script para sincronización de estados componentes de medidodor pr_component vs compsesu
y anulación de ordenes duplicadas
    
    Archivo de entrada 
    ===================
    NA            
    
    Archivo de Salida 
    ===================
    WO0000000958309_yyyymmdd_Log.txt
    WO0000000958309_yyyymmdd_Poblacion.txt
    WO0000000958309_yyyymmdd_Ordenes.txt
    
    --Modificaciones    
    
    18/03/2021 - jcatuche
    Creación
    
    
    
       
    
    
***********************************************************/
declare
    --Constantes
    csbWO               constant varchar2( 32 ) := 'WO0000000958309';
    csbEPM_WO           constant varchar2( 10 ) := substr(csbWO,1,2)||trim(leading '0' from substr(csbWO,3,7))||substr(csbWO,10);
    csbdOutPut          constant varchar2( 1 )  := 'N';
    csbfInPut           constant varchar2( 1 )  := 'N';
    csbEscritura        constant varchar2( 1 )  := 'w';
    csbLectura          constant varchar2( 1 )  := 'r';
    csbPIPE             constant varchar2( 1 )  := '|';
    cdtFecha            constant date           := sysdate; --ut_date.fdtSysdate;
    csbformato          constant varchar2( 50 ) := 'dd/mm/yyyy hh24:mi:ss';
    csbformatos         constant varchar2( 50 ) := 'yyyymmdd'; --'yyyymmdd_hh24miss';
    cdtFechRepo         constant date           := to_date('01/01/2001','dd/mm/yyyy');
    cnuSegundo          constant number         := 1/86400;
    cnuLimit            constant number         := 500;
    cnuIdErr            constant number         := 1;
    cnuHilo             constant number         := 1;   
    cnuOuts             constant number         := 3;
    cnuHash             constant number         := 10;
    
    
    
    --Tipos de Registro
    type tyrcOrden is record
    (
        orderid         or_order.order_id%type,
        ostatusid       or_order.order_status_id%type,
        ostatusid_n     or_order.order_status_id%type,
        ostatusdesc     or_order_status.description%type,
        ostatusfin      or_order_status.is_final_status%type,
        causalid        ge_causal.causal_id%type,
        causaldesc      ge_causal.description%type,
        classcausalid   ge_class_causal.class_causal_id%type,
        classcausaldesc ge_class_causal.description%type,
        createddate     or_order.created_date%type,
        assigneddate    or_order.assigned_date%type,
        legalizadate    or_order.legalization_date%type,
        orderactivityid or_order_activity.order_activity_id%type,
        status          or_order_activity.status%type,
        status_n        or_order_activity.status%type,
        tasktypeid      or_task_type.task_type_id%type,
        tasktypedesc    or_task_type.description%type,
        componentid     or_order_activity.component_id%type,
        componentprod   mo_component.component_id_prod%type,
        sbActualiza     varchar2(1)
    );
    type tytbOrden is table of tyrcOrden index by binary_integer;  

    
    type tyrcRegistro is record 
    (
        sesunuse    servsusc.sesunuse%type,
        emsscoem    elmesesu.emsscoem%type,
        susccodi    suscripc.susccodi%type,
        suscnomb    suscripc.suscmail%type,    
        clienomb    ge_subscriber.subscriber_name%type, 
        clienit     ge_subscriber.identification%type,
        sesuserv    servsusc.sesuserv%type,
        servdesc    servicio.servdesc%type,
        sesuesco    servsusc.sesuesco%type,
        escodesc    estacort.escodesc%type,
        sesucicl    servsusc.sesucicl%type,
        sesucico    servsusc.sesucico%type,
        sesufein    servsusc.sesufein%type,
        sesufere    servsusc.sesufere%type,
        sesufucb    servsusc.sesufucb%type,
        sesudepa    servsusc.sesudepa%type,
        sesuloca    servsusc.sesuloca%type,
        sesucate    servsusc.sesucate%type,
        sesusuca    servsusc.sesusuca%type,
        sesuplfa    servsusc.sesuplfa%type,
        commerplan  pr_product.commercial_plan_id%type,
        addressp    ab_address.address_parsed%type,
        addressc    ab_address.address_parsed%type,
        sbFlag      varchar2(1),
        cmssidco    compsesu.cmssidco%type,
        cmssidcp    compsesu.cmssidcp%type,
        cmssescm    compsesu.cmssescm%type,
        escmdesc    ps_product_status.description%type,
        cmsstcom    compsesu.cmsstcom%type,
        tcomdesc    ps_component_type.description%type,
        cmssclse    compsesu.cmssclse%type,
        clsedesc    ps_class_service.description%type,
        cmssfein    compsesu.cmssfein%type,
        cmssfere    compsesu.cmssfere%type,
        cmsscouc    compsesu.cmsscouc%type,
        cstatusid   pr_component.component_status_id%type,
        cstatusid_n pr_component.component_status_id%type,
        sbActualiza varchar2(1),
        packageid   mo_packages.package_id%type,
        motiveid    mo_motive.motive_id%type,
        pstatusid   ps_motive_status.motive_status_id%type,
        pstatusdesc ps_motive_status.description%type,
        pstatusfin  ps_motive_status.is_final_status%type,
        prqstdate   mo_packages.request_date%type,
        ptypeid     ps_package_type.package_type_id%type,
        ptypedesc   ps_package_type.description%type,
        ptagname    ps_package_type.tag_name%type,
        activityid  or_order_activity.activity_id%type,
        instanceid  or_order_activity.instance_id%type,
        cantorden   number,
        tbOrdenes   tytbOrden    
     );
    type tytbRegistro is table of tyrcRegistro index by varchar2(10);
    
    tbRegistro  tytbRegistro;
    sbHash      varchar2(10);
    nuHash      number;
    
    type tyrcArchivos is record
    (
        cabecera    varchar2(2000),
        nombrearch  varchar2(60),
        tipoarch    varchar2(1),
        flgprint    varchar2(1),
        flFile      utl_file.file_type
    );
    type tytbArchivos is table of tyrcArchivos index by binary_integer;
    tbArchivos          tytbArchivos;
    
    
    type tyTabla is table of varchar2( 2000 ) index by binary_integer;
    
    --Variables
    --tbCampos            pkg_epm_utilidades.tyTabla;
    tbCampos            tyTabla;
    sbRuta              parametr.pamechar%type;              
    nuLine              number;
    nuTotal             number;
    nuOk                number;
    nuOr                number;
    nuAct               number;
    nuWrng              number;
    nuErr               number;
    sbCabecera          varchar2(2000);
    osbline             varchar2(2000);
    s_Linea_out         varchar2(2000);
    s_Linea_outc        varchar2(2000);
    raise_continuar     exception;
    sbComentario        varchar2(2000);
    nuContador          number;
    nuPivote            number;
    nuServicio          servsusc.sesunuse%type;
    nuComponente        compsesu.cmssidco%type;
    nuErrorCode         number;
    sbErrorMensaje      varchar2(2000);

    
    
    --Cursores
    cursor cuLecManual is 
    select /*+ index ( a IDX_AUDIT_COMPSESU_01 ) */ 
    cmssidco,o_cmsssesu sesunuse 
    from audit_compsesu a
    where a.current_program_name = 'WO284735'
    and o_cmssescm != n_cmssescm
    and cmssidco in 
    (
        230711963,230711796,230714500,230714421,230707002,230714081,230714135,230714413,
        230714698,230714092,230711863,230713137,230717461,230716616,230711581,230713822,
        230716871,230722073,230717871,230718852,230717142,230714245,230713616,230714126,
        230712863,230713177,230712550,230714278,237284208,230711799,230713858,230716965,
        230713188,230714166,230717203,230712227,230713625,234220887,234226084,234273980,
        234376627,234504018,234508114,234561929,234641093,234667090,234656657,234662534,
        234668824,234679484,234683973,234691734,234706269,234711637,234732124,234731270,
        234786796,234782993,234805528,234812545,234838864,234843109,234838313,234896618,
        234961537,234973215,235026138,235036750,235043879,235131660,235100164,235112165,
        235145808,235152628,235177818,235187703,235254480,235283645,235286535,235621854,
        235968568,235968594,235969044,235968457,236013703,236094014,236094626,236210298,
        236267857,236545561,236639943,236646437,236711061,236732563,236783140,236817807,
        236823629,236830468,236836652,236861842,236864738,240875105,236899995,236945734,
        236994478,237216475,237272697,237271824,237279896,237286476,237325274,237337163,
        237350256,235282926,237575925,237567818,237601985,237600888,237793951,238409627,
        237971292,238106356,238232037
    ) 
    --and o_cmsssesu = 94422580
    ;

    type tytbLecManual is table of cuLecManual%rowtype index by binary_integer;
    tbLecManual     tytbLecManual;
    
    -- pkg_epm_utilidades.ParseString
    PROCEDURE ParseString
    (
        ivaCadena  IN      VARCHAR2,
        ivaToken   IN      VARCHAR2,
        otbSalida  OUT     tyTabla 
    ) 
    IS
        nuIniBusqueda     NUMBER          := 1;
        nuFinBusqueda     NUMBER          := 1;
        sbArgumento       VARCHAR2( 2000 );
        nuIndArgumentos   NUMBER          := 1;
        nuLongitudArg     NUMBER;
    BEGIN
        -- Recorre la lista de argumentos y los guarda en un tabla pl-sql
        WHILE( ivaCadena IS NOT NULL ) LOOP
        
            -- Busca el separador en la cadena y almacena su posicion
            nuFinBusqueda := INSTR( ivaCadena, ivaToken, nuIniBusqueda );

            -- Si no exite el pipe, debe haber un argumento
            IF ( nuFinBusqueda = 0 ) THEN
                -- Obtiene el argumento
                sbArgumento := SUBSTR( ivaCadena, nuIniBusqueda );

                -- Si existe el argumento lo almacena en la tabla de argumentos
                IF ( sbArgumento IS NOT NULL ) THEN
                    otbSalida( nuIndArgumentos ) := sbArgumento;
                END IF;

                -- Termina el ciclo
                EXIT;
            END IF;

            -- Obtiene el argumento hasta el separador
            nuLongitudArg := nuFinBusqueda - nuIniBusqueda;
            
            -- Obtiene argumento
            sbArgumento := SUBSTR( ivaCadena, nuIniBusqueda, nuLongitudArg );
            
            -- Lo adiciona a la tabla de argumentos, quitando espacios y ENTER a los lados
            otbSalida( nuIndArgumentos ) := TRIM( REPLACE( sbArgumento, CHR( 13 ), '' ));
            
            -- Inicializa la posicion inicial con la posicion del caracterer
            -- despues del pipe
            nuIniBusqueda := nuFinBusqueda + 1;
            
            -- Incrementa el indice de la tabla de argumentos
            nuIndArgumentos := nuIndArgumentos + 1;
            
        END LOOP;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE;
    END ParseString;
    
    FUNCTION fnc_rs_CalculaTiempo
    (
        idtFechaIni IN DATE,
        idtFechaFin IN DATE
    )
    RETURN VARCHAR2
    IS
        nuTiempo NUMBER;
        nuHoras NUMBER;
        nuMinutos NUMBER;
        sbRetorno VARCHAR2( 100 );
    BEGIN
        -- Convierte los dias en segundos
        nuTiempo := ( idtFechaFin - idtFechaIni ) * 86400;
        -- Obtiene las horas
        nuHoras := TRUNC( nuTiempo / 3600 );
        -- Publica las horas
        sbRetorno := TO_CHAR( nuHoras ) ||'h ';
        -- Resta las horas para obtener los minutos
        nuTiempo := nuTiempo - ( nuHoras * 3600 );
        -- Obtiene los minutos
        nuMinutos := TRUNC( nuTiempo / 60 );
        -- Publica los minutos
        sbRetorno := sbRetorno ||TO_CHAR( nuMinutos ) ||'m ';
        -- Resta los minutos y obtiene los segundos redondeados a dos decimales
        nuTiempo := TRUNC( nuTiempo - ( nuMinutos * 60 ), 2 );
        -- Publica los segundos
        sbRetorno := sbRetorno ||TO_CHAR( nuTiempo ) ||'s';
        -- Retorna el tiempo
        RETURN( sbRetorno );
    EXCEPTION
        WHEN OTHERS THEN
            -- No se eleva excepcion, pues no es parte fundamental del proceso
            RETURN NULL;
    END fnc_rs_CalculaTiempo;
    
    --pkg_epm_filemanager.fgetnextline
    FUNCTION fGetNextLine
    (
        iflFileHandle IN UTL_FILE.FILE_TYPE,
        osbLine     OUT VARCHAR2
    ) return boolean 
    is
        blEndFile boolean := false;
    begin
         utl_file.get_line ( iflFileHandle, osbLine );
         return blEndFile;  
    exception
        when no_data_found then
             osbLine  := null;
             blEndFile:= true;
             return blEndFile; 
    end fGetNextLine;  
    
    PROCEDURE pInicializar IS
    BEGIN
        
        BEGIN
            --EXECUTE IMMEDIATE
            --'alter session set nls_date_format = "dd/mm/yyyy hh24:mi:ss"';
            
            EXECUTE IMMEDIATE 
            'alter session set nls_numeric_characters = ",."';
            
        END;

        IF csbdOutPut = 'S' THEN
            dbms_output.enable;
            dbms_output.enable (buffer_size => null);
        END IF;
        
        --pkerrors.setapplication(csbEPM_WO);
        --Sa_BOSystem.SetSystemProcessName(csbEPM_WO);    
        select pamechar into sbRuta from parametr where pamecodi = 'RUTA_TRAZA';
        --sbRuta := pkGeneralParametersMgr.fsbGetStringValue('RUTA_TRAZA');

        nuLine      := 0;
        nuTotal     := 0;
        nuOk        := 0;
        nuOr        := 0;
        nuAct       := 0;
        nuErr       := 0;      
        nuWrng      := 0;  
        
        tbRegistro.delete;
        tbcampos.delete;
        tbLecManual.delete;
        tbArchivos.delete; 

        tbArchivos(-1).nombrearch   := csbWO||'_'||cnuHilo||'.txt';
        tbArchivos(0).nombrearch    := csbWO||'_In.txt';
        tbArchivos(1).nombrearch    := csbWO||'_'||to_char(cdtFecha,csbformatos)||'_Log.txt';
        tbArchivos(2).nombrearch    := csbWO||'_'||to_char(cdtFecha,csbformatos)||'_Poblacion.txt';
        tbArchivos(3).nombrearch    := csbWO||'_'||to_char(cdtFecha,csbformatos)||'_Ordenes.txt';
        
        tbArchivos(-1).tipoarch := csbEscritura;
        tbArchivos(0).tipoarch :=  csbLectura;
        tbArchivos(1).tipoarch :=  csbEscritura;
        tbArchivos(2).tipoarch :=  csbEscritura;
        tbArchivos(3).tipoarch :=  csbEscritura; 
        --tbArchivos(4).tipoarch := csbEscritura;

        tbArchivos(-1).flgprint := 'S';
        tbArchivos(0).flgprint  := csbfInPut;
        tbArchivos(1).flgprint  := 'N';
        tbArchivos(2).flgprint  := 'N';
        tbArchivos(3).flgprint  := 'N';
        
        sbCabecera := 'TipoError|Producto|Componente|Mensaje|Error'; 
        tbArchivos(cnuIdErr).cabecera := sbCabecera;
        
        sbCabecera := 'Sesunuse|Emsscoem|Susccodi|Suscname|Clienomb|Identidad|Sesuserv|Servdesc|Sesuesco|Escodesc|Sesucicl|Sesucico|Sesufein|Sesufere|Sesufucb|Sesudepa|Sesuloca|Sesucate|Sesusuca|Sesuplfa|CommerPlan|AddressP|AddressC|';
        sbCabecera := sbCabecera||'Cmssidco|Cmssidcp|Cmssescm|Escmdesc|Cmsstcom|Tcomdesc|Cmssclse|Clsedesc|Cmssfein|Cmssfere|Cmsscouc|';
		sbCabecera := sbCabecera||'Component_status_ant|Component_status_act|Actualizado';
        tbArchivos(2).cabecera := sbCabecera;
        
        sbCabecera := 'Sesunuse|Cmssidco|PackageId|MotiveId|PstatusId|PstatusDesc|PtypeId|PtypeDesc|Ptagname|PrequestDate|Activity|Instance|CantOrden|';
        sbCabecera := sbCabecera||'OrdenId|TaskTypeId|TaskTypeDesc|CausalId|CausalDesc|CreateDate|AssignedDate|LegalizeDate|OrderActivity|';
        sbCabecera := sbCabecera||'OcomponentId|ComponentId|OstatusId_ant|Ostatusdesc|OstatusId_act|Status_ant|Status_act|Actualizado';
        tbArchivos(3).cabecera := sbCabecera;

    END pInicializar;
    
    PROCEDURE pCustomOutput(sbDatos in varchar2) is
        loop_count  number default 1;
        string_length number;    
    begin 
        string_length := length (sbDatos);
        
        while loop_count < string_length loop 
            dbms_output.put(substr (sbDatos,loop_count,255));
            --dbms_output.new_line;
            loop_count := loop_count +255;  
        end loop;
        dbms_output.new_line;
    exception
        when others then
        null;                  
    END pCustomOutput;
    
    Procedure pEscritura (ircArchivos  in out tyrcArchivos, sbMensaje  in varchar2) IS
    Begin 
        If csbdOutPut = 'S' THEN 
            if ircArchivos.flgprint = 'S' then
                pCustomOutput(sbMensaje); 
            end if;
        Else
            Utl_file.put_line(ircArchivos.flFile,sbMensaje,TRUE);
            Utl_file.fflush(ircArchivos.flFile);
        End if;
    exception
        when others then
        sbComentario := 'Error escritura archivo';
        raise raise_continuar;  
    END pEscritura;

    Procedure pOpen(inuOut in number) IS
    Begin
        if csbdOutPut != 'S' then
            if inuOut = 0 and tbArchivos(inuOut).flgprint = 'S' then
                -- Archivo de Entrada
                --pkg_epm_filemanager.pClearFileCtrlM(sbRuta, tbArchivos(inuOut).nombrearch);
                --tbArchivos(inuOut).flFile := pkg_epm_gestionarchivos.ffabrirarchivo(sbRuta, tbArchivos(inuOut).nombrearch, tbArchivos(inuOut).tipoarch);
                tbArchivos(inuOut).flFile := utl_file.fopen(sbRuta, tbArchivos(inuOut).nombrearch, tbArchivos(inuOut).tipoarch);
            elsif inuOut != 0 then
                --tbArchivos(inuOut).flFile := pkg_epm_gestionarchivos.ffabrirarchivo(sbRuta, tbArchivos(inuOut).nombrearch, tbArchivos(inuOut).tipoarch);
                tbArchivos(inuOut).flFile := utl_file.fopen(sbRuta, tbArchivos(inuOut).nombrearch, tbArchivos(inuOut).tipoarch);
            end if;        
        end if;
    exception
        when others then
            raise;    
    End pOpen;
     
    PROCEDURE pAbrirArchivo IS
    BEGIN
        for i in -1 .. cnuOuts loop
            begin   
                pOpen(i);                    
                if i >= cnuIdErr then
                    pEscritura(tbArchivos(i),tbArchivos(i).cabecera);
                    pEscritura(tbArchivos(-1),tbArchivos(i).nombrearch);
                end if;
            exception
                when utl_file.invalid_operation then
                    if utl_file.is_open( tbArchivos(cnuIdErr).flFile ) then
                        sbComentario := 'Error -1|||Error en operacion "'||tbArchivos(i).tipoarch||
                        '" para el archivo "'||tbArchivos(i).nombrearch||'" en la ruta "'||sbRuta||'"|'||sqlerrm;
                        pEscritura(tbArchivos(cnuIdErr),sbComentario);
                        raise;
                    else
                        dbms_output.put_line('Error -1|||Error no controlado el apertura de archivos|'||sqlerrm);    
                        raise;
                    end if;
            end;                   
        end loop;
    END pAbrirArchivo;
    
    PROCEDURE pCerrarArchivo IS
    BEGIN
        pEscritura(tbArchivos(cnuIdErr),'================================================');
        pEscritura(tbArchivos(cnuIdErr),'Finaliza la actualización '||csbWO||'.' );
        pEscritura(tbArchivos(cnuIdErr),'Total de Componentes detectados : '||nuLine);
        pEscritura(tbArchivos(cnuIdErr),'Total de Componentes almacenados : '||nuTotal);
        pEscritura(tbArchivos(cnuIdErr),'Total de Componenes actualizados : '||nuOk);
        pEscritura(tbArchivos(cnuIdErr),'Total de Ordenes actualizadas : '||nuOr);
        pEscritura(tbArchivos(cnuIdErr),'Total de Actividades actualizadas : '||nuAct);
        pEscritura(tbArchivos(cnuIdErr),'Total de Advertencias : '||nuWrng);
        pEscritura(tbArchivos(cnuIdErr),'Total de Errores : '||nuErr);
        pEscritura(tbArchivos(cnuIdErr),'Rango Total de Ejecución ['||to_char(cdtFecha,'dd/mm/yyyy')||']['||to_char(cdtFecha,'hh24:mi:ss')||' - '||to_char(sysdate,'hh24:mi:ss')||']');
        pEscritura(tbArchivos(cnuIdErr),'Tiempo Total de Ejecución['||fnc_rs_CalculaTiempo(cdtFecha,sysdate)||']');
        
        for i in -1 .. cnuOuts loop
            if ( utl_file.is_open( tbArchivos(i).flFile ) ) then
                utl_file.fclose( tbArchivos(i).flFile );
            end if;
        end loop;
    END pCerrarArchivo;
    
    PROCEDURE pCerrarArchivoE IS
    BEGIN
        -- Indica que terminó el proceso en el archivo de salida
        pEscritura(tbArchivos(cnuIdErr),'================================================');
        pEscritura(tbArchivos(cnuIdErr),'Finaliza la actualización con Error '||csbWO||': ' ||sqlerrm );
        pEscritura(tbArchivos(cnuIdErr),'Total de Componentes detectados : '||nuLine);
        pEscritura(tbArchivos(cnuIdErr),'Total de Componentes almacenados : '||nuTotal);
        pEscritura(tbArchivos(cnuIdErr),'Total de Componenes actualizados : '||nuOk);
        pEscritura(tbArchivos(cnuIdErr),'Total de Ordenes actualizadas : '||nuOr);
        pEscritura(tbArchivos(cnuIdErr),'Total de Actividades actualizadas : '||nuAct);
        pEscritura(tbArchivos(cnuIdErr),'Total de Advertencias : '||nuWrng);
        pEscritura(tbArchivos(cnuIdErr),'Total de Errores : '||nuErr);
        pEscritura(tbArchivos(cnuIdErr),'Rango Total de Ejecución ['||to_char(cdtFecha,'dd/mm/yyyy')||']['||to_char(cdtFecha,'hh24:mi:ss')||' - '||to_char(sysdate,'hh24:mi:ss')||']');
        pEscritura(tbArchivos(cnuIdErr),'Tiempo Total de Ejecución['||fnc_rs_CalculaTiempo(cdtFecha,sysdate)||']');
        
        for i in -1 .. cnuOuts loop
            if ( utl_file.is_open( tbArchivos(i).flFile ) ) then
                utl_file.fclose( tbArchivos(i).flFile );
            end if;
        end loop;
    END pCerrarArchivoE;
    
    PROCEDURE pGeneraciondeRastro(ircRecord in out nocopy tyrcRegistro) is
    BEGIN
        s_Linea_out := nuServicio;
        s_Linea_out := s_Linea_out||'|'||ircRecord.emsscoem;
        s_Linea_out := s_Linea_out||'|'||ircRecord.susccodi;
        s_Linea_out := s_Linea_out||'|'||ircRecord.suscnomb;
        s_Linea_out := s_Linea_out||'|'||ircRecord.clienomb;
        s_Linea_out := s_Linea_out||'|'||ircRecord.clienit;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesuserv;
        s_Linea_out := s_Linea_out||'|'||ircRecord.servdesc;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesuesco;
        s_Linea_out := s_Linea_out||'|'||ircRecord.escodesc;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesucicl;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesucico;
        s_Linea_out := s_Linea_out||'|'||to_char(ircRecord.sesufein,csbFormato);
        s_Linea_out := s_Linea_out||'|'||to_char(ircRecord.sesufere,csbFormato);
        s_Linea_out := s_Linea_out||'|'||to_char(ircRecord.sesufucb,csbFormato);
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesudepa;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesuloca;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesucate;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesusuca;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sesuplfa;
        s_Linea_out := s_Linea_out||'|'||ircRecord.commerplan;
        s_Linea_out := s_Linea_out||'|'||ircRecord.addressp; 
        s_Linea_out := s_Linea_out||'|'||ircRecord.addressc; 
        s_Linea_out := s_Linea_out||'|'||nuComponente;
        s_Linea_out := s_Linea_out||'|'||ircRecord.cmssidcp;  
        s_Linea_out := s_Linea_out||'|'||ircRecord.cmssescm;
        s_Linea_out := s_Linea_out||'|'||ircRecord.escmdesc;
        s_Linea_out := s_Linea_out||'|'||ircRecord.cmsstcom;
        s_Linea_out := s_Linea_out||'|'||ircRecord.tcomdesc;
        s_Linea_out := s_Linea_out||'|'||ircRecord.cmssclse;
        s_Linea_out := s_Linea_out||'|'||ircRecord.clsedesc;
        s_Linea_out := s_Linea_out||'|'||to_char(ircRecord.cmssfein,csbFormato);
        s_Linea_out := s_Linea_out||'|'||to_char(ircRecord.cmssfere,csbFormato);
        s_Linea_out := s_Linea_out||'|'||ircRecord.cmsscouc;
        s_Linea_out := s_Linea_out||'|'||ircRecord.cstatusid;
        s_Linea_out := s_Linea_out||'|'||ircRecord.cstatusid_n;
        s_Linea_out := s_Linea_out||'|'||ircRecord.sbActualiza;

        pEscritura(tbArchivos(2),s_Linea_out);
        
        if ircRecord.packageid is not null then
            s_Linea_out := nuServicio;
            s_Linea_out := s_Linea_out||'|'||nuComponente;
            s_Linea_out := s_Linea_out||'|'||ircRecord.packageid;
            s_Linea_out := s_Linea_out||'|'||ircRecord.motiveid;
            s_Linea_out := s_Linea_out||'|'||ircRecord.pstatusid;
            s_Linea_out := s_Linea_out||'|'||ircRecord.pstatusdesc||' ['||ircRecord.pstatusfin||']';
            s_Linea_out := s_Linea_out||'|'||ircRecord.ptypeid;
            s_Linea_out := s_Linea_out||'|'||ircRecord.ptypedesc;
            s_Linea_out := s_Linea_out||'|'||ircRecord.ptagname;
            s_Linea_out := s_Linea_out||'|'||to_char(ircRecord.prqstdate,csbFormato);
            s_Linea_out := s_Linea_out||'|'||ircRecord.activityid;
            s_Linea_out := s_Linea_out||'|'||ircRecord.instanceid;
            s_Linea_out := s_Linea_out||'|'||ircRecord.cantorden;

            nuHash := ircRecord.tbOrdenes.first;
            
            if nuHash is not null then
                for nuHash in ircRecord.tbOrdenes.first..ircRecord.tbOrdenes.last loop
                    s_Linea_outc := ircRecord.tbOrdenes(nuHash).orderid;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).tasktypeid;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).tasktypedesc;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).causalid;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).causaldesc||' ['||ircRecord.tbOrdenes(nuHash).classcausalid||' - '||ircRecord.tbOrdenes(nuHash).classcausaldesc||']';
                    s_Linea_outc := s_Linea_outc||'|'||to_char(ircRecord.tbOrdenes(nuHash).createddate,csbFormato);
                    s_Linea_outc := s_Linea_outc||'|'||to_char(ircRecord.tbOrdenes(nuHash).assigneddate,csbFormato);
                    s_Linea_outc := s_Linea_outc||'|'||to_char(ircRecord.tbOrdenes(nuHash).legalizadate,csbFormato);
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).orderactivityid;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).componentid;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).componentprod;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).ostatusid;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).ostatusdesc||' ['||ircRecord.tbOrdenes(nuHash).ostatusfin||']';
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).ostatusid_n;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).status;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).status_n;
                    s_Linea_outc := s_Linea_outc||'|'||ircRecord.tbOrdenes(nuHash).sbActualiza;

                    pEscritura(tbArchivos(3),s_Linea_out||'|'||s_Linea_outc);

                end loop; 
            else 
                s_Linea_outc := '';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';
                s_Linea_outc := s_Linea_outc||'|';

                pEscritura(tbArchivos(3),s_Linea_out||'|'||s_Linea_outc);
            end if;
        end if;

        
    exception
        when others then
            sbErrorMensaje := sqlerrm;
            --pkerrors.geterrorvar (nuErrorCode, sbErrorMensaje);
            sbComentario := 'Error 3.0|'||nuServicio||'|'||nuComponente||
            '|Error en impresión de información|'||sbErrorMensaje;
            raise raise_continuar;                      
    END pGeneraciondeRastro; 
  
    PROCEDURE pActualizaDatos(ircRecord in out nocopy tyrcRegistro) is
    BEGIN 
        /*update pr_component
        set component_status_id = ircRecord.cstatusid_n
        where product_id = nuServicio
        and component_id = nuComponente;*/

        --if sql%rowcount > 0 then
        if true then
            ircRecord.sbActualiza := 'S';
            nuOk := nuOk + 1;
        else
            sbComentario := 'Wrng 2.1|'||nuServicio||'|'||nuComponente||
            '|Estado del componente no actualizado ['||ircRecord.cstatusid||' - '||ircRecord.cstatusid_n||']|NA';
            pEscritura(tbArchivos(cnuIdErr),sbComentario);
            nuWrng := nuWrng + 1;  

            ircRecord.cstatusid_n := ircRecord.cstatusid; 
        end if;

        nuHash := 0;
        if not ircRecord.tbOrdenes.exists(nuHash) and ircRecord.packageid is not null then
            sbComentario := 'Wrng 2.2|'||nuServicio||'|'||nuComponente||
            '|Producto sin orden para anular asociada al componente retirado. Solicitud ['||ircRecord.packageid||'] Motivo ['||ircRecord.motiveid||']|NA';
            pEscritura(tbArchivos(cnuIdErr),sbComentario);
            nuWrng := nuWrng + 1;  
        elsif ircRecord.packageid is not null and ircRecord.cantorden != 1 then
            
            if ircRecord.tbOrdenes(nuHash).ostatusid != 0 or ircRecord.tbOrdenes(nuHash).status = 'F'  then
                sbComentario := 'Wrng 2.3|'||nuServicio||'|'||nuComponente||
                '|Producto con orden para anular en un estado diferente al esperado. Orden ['||ircRecord.tbOrdenes(nuHash).orderid||
                '] Estado ['||ircRecord.tbOrdenes(nuHash).ostatusid||' - '||ircRecord.tbOrdenes(nuHash).ostatusdesc||']|NA';
                pEscritura(tbArchivos(cnuIdErr),sbComentario);
                nuWrng := nuWrng + 1;      
            else
                --Anulación 
                /*update or_order
                set order_status_id = ircRecord.tbOrdenes(nuHash).ostatusid_n
                where order_id = ircRecord.tbOrdenes(nuHash).orderid;*/

                --if sql%rowcount > 0 then
                if true then

                    /*update or_order_activity
                    set status = ircRecord.tbOrdenes(nuHash).status_n
                    where order_id = ircRecord.tbOrdenes(nuHash).orderid
                    and motive_id = ircRecord.motiveid
                    and product_id = nuServicio
                    and order_activity_id = ircRecord.tbOrdenes(nuHash).orderactivityid;*/

                    --if sql%rowcount > 0 then
                    if true then
                        ircRecord.tbOrdenes(nuHash).sbActualiza := 'S';
                        nuOr := nuOr + 1;
                        nuAct := nuAct + 1;
                    else
                        sbComentario := 'Wrng 2.5|'||nuServicio||'|'||nuComponente||
                        '|Estado de la actividad no actualizada ['||ircRecord.tbOrdenes(nuHash).orderactivityid||']['||ircRecord.tbOrdenes(nuHash).status||'
                        ]['||ircRecord.tbOrdenes(nuHash).status_n||']|NA';
                        pEscritura(tbArchivos(cnuIdErr),sbComentario);
                        nuWrng := nuWrng + 1;   

                        ircRecord.tbOrdenes(nuHash).status_n := ircRecord.tbOrdenes(nuHash).status;
                        nuOr := nuOr + 1;
                    end if;
                else
                    sbComentario := 'Wrng 2.4|'||nuServicio||'|'||nuComponente||
                    '|Estado de la orden no actualizada ['||ircRecord.tbOrdenes(nuHash).orderid||']['||ircRecord.tbOrdenes(nuHash).ostatusid||'
                    ]['||ircRecord.tbOrdenes(nuHash).ostatusid_n||']|NA';
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    nuWrng := nuWrng + 1;   

                    ircRecord.tbOrdenes(nuHash).ostatusid_n := ircRecord.tbOrdenes(nuHash).ostatusid;
                    ircRecord.tbOrdenes(nuHash).status_n := ircRecord.tbOrdenes(nuHash).status;
                end if;     
            end if;
        end if;

        commit;
        
    exception
        when raise_continuar then   
            rollback;
            raise; 
        when others then
			sbErrorMensaje := sqlerrm;
            --pkerrors.geterrorvar (nuErrorCode, sbErrorMensaje);
            sbComentario := 'Error 2.0|'||nuServicio||'|'||nuComponente||
            '|Error en actualización de datos|'||sbErrorMensaje;
            rollback;
            raise raise_continuar;                      
    END pActualizaDatos; 
    
    PROCEDURE pAnalizaDatos is
        
        cursor cuServicio (inusesu in servsusc.sesunuse%type) is 
        select /*+ index (s pk_servsusc) index (p pk_pr_product) */
        sesunuse,sesuserv,servdesc,susccodi,replace(suscmail,'|','/') suscmail,replace(subscriber_name,'|','/') subscriber_name,identification,sesucicl,sesucico,sesuesco,escodesc,sesufein,sesufere,sesufucb,
        sesucate,sesusuca,sesudepa,sesuloca,sesuplfa,commercial_plan_id commerplan,
        (select replace(address_parsed,'|','/') from ab_address where address_id = p.address_id) addressp,
        (select replace(address_parsed,'|','/') from ab_address where address_id = susciddi) addressc
        from servsusc s, pr_product p, suscripc, ge_subscriber gs,servicio,estacort
        where sesunuse = inusesu
        and product_id = sesunuse
        and sesususc = susccodi 
        and servcodi = sesuserv
        and escocodi = sesuesco
        and subscriber_id = suscclie;

        rcServicio  cuServicio%rowtype;

        cursor cuElemento (inusesu in servsusc.sesunuse%type, inucomp in compsesu.cmssidco%type) is  
        select * from elmesesu
        where emsssesu = inusesu
        and emsscmss != inucomp
        order by emssfere desc;

        rcElemento  cuElemento%rowtype;

        cursor cuComponente (inusesu in servsusc.sesunuse%type, inucomp in compsesu.cmssidco%type) is
        select c.cmsssesu,c.cmssidco,p.component_id,c.cmssidcp,cmssescm,pc.description escmdesc,component_status_id,ps.description,
        cmsstcom,p.component_type_id,ct.description tcomdesc,cmssclse,p.class_service_id,cs.description clsedesc,cmssfein,cmssfere,
        replace(replace(replace(cmsscouc,'|','/'),chr(10),' '),chr(13)) cmsscouc
        from compsesu c,pr_component p,ps_product_status ps,ps_product_status pc,ps_component_type ct,ps_class_service cs
        where product_id = inusesu
        and component_id = inucomp
        and cmsssesu = product_id
        and c.cmssidco = component_id
        and c.cmsstcom = ct.component_type_id
        and c.cmssclse = cs.class_service_id(+)
        and c.cmssescm = pc.PRODUCT_STATUS_ID
        and p.COMPONENT_STATUS_ID = ps.PRODUCT_STATUS_ID
        order by c.cmssidco;        

        rcComponente    cuComponente%rowtype;

        cursor cuSolicitud (insesu in servsusc.sesunuse%type) is 
        select m.package_id,m.motive_status_id,ms.DESCRIPTION PstatusDesc,ms.IS_FINAL_STATUS,
        m.package_type_id,pt.DESCRIPTION PtypeDesc,pt.TAG_NAME,m.REQUEST_DATE,oa.activity_id,
        oa.instance_id,oa.product_id,oa.MOTIVE_ID,count(oa.order_id) cantidad
        from or_order_activity oa, mo_packages m, ps_motive_status ms,ps_package_type pt
        where 1 = 1
        and oa.product_id = insesu
        and oa.package_id = m.package_id
        and ms.MOTIVE_STATUS_ID = m.MOTIVE_STATUS_ID
        and pt.package_type_id = m.package_type_id
        and origin_activity_id is null
        and exists
        (
            select 'x' from ps_motive_status
            where motive_status_id = m.motive_status_id
            and is_final_status = 'N'
        )
        group by m.package_id,m.motive_status_id,ms.description,ms.is_final_status,m.package_type_id,pt.DESCRIPTION,
        pt.TAG_NAME,m.request_date,oa.activity_id,oa.instance_id,oa.product_id,oa.motive_id;

        rcSolicitud       cuSolicitud%rowtype;

        cursor cuOrdenret (inumotive in mo_motive.motive_id%type, inucomp in compsesu.cmssidco%type) is
        select o.order_id,o.ORDER_STATUS_ID ostatusid,os.description ostatusdesc,os.IS_FINAL_STATUS ostatusfin,o.CAUSAL_ID,gc.DESCRIPTION causaldesc,
        gc.CLASS_CAUSAL_ID,(select gcc.description from ge_class_causal gcc where gcc.CLASS_CAUSAL_ID = gc.class_causal_id) classcausaldesc,
        o.CREATED_DATE,o.ASSIGNED_DATE,o.LEGALIZATION_DATE,
        oa.order_activity_id,oa.status,oa.task_type_id,ot.DESCRIPTION tasktypedesc,oa.component_id,mc.COMPONENT_ID_PROD
        from or_order_activity oa,mo_component mc, or_order o,ge_causal gc,or_order_status os,or_task_type ot
        where oa.motive_id = inumotive
        and oa.COMPONENT_ID = mc.COMPONENT_ID
        and o.order_id = oa.order_id
        and gc.causal_id(+) = o.causal_id
        and os.order_status_id = o.order_status_id
        and ot.TASK_TYPE_ID = o.TASK_TYPE_ID
        and oa.ORIGIN_ACTIVITY_ID is null
        and mc.COMPONENT_ID_PROD = inucomp;

        rcOrdenret      cuOrdenret%rowtype;

        cursor cuOrdenact (inumotive in mo_motive.motive_id%type, inucomp in compsesu.cmssidco%type) is
        select o.order_id,o.ORDER_STATUS_ID ostatusid,os.description ostatusdesc,os.IS_FINAL_STATUS ostatusfin,o.CAUSAL_ID,gc.DESCRIPTION causaldesc,
        gc.CLASS_CAUSAL_ID,(select gcc.description from ge_class_causal gcc where gcc.CLASS_CAUSAL_ID = gc.class_causal_id) classcausaldesc,
        o.CREATED_DATE,o.ASSIGNED_DATE,o.LEGALIZATION_DATE,
        oa.order_activity_id,oa.status,oa.task_type_id,ot.DESCRIPTION tasktypedesc,oa.component_id,mc.COMPONENT_ID_PROD
        from or_order_activity oa,mo_component mc, or_order o,ge_causal gc,or_order_status os,or_task_type ot
        where oa.motive_id = inumotive
        and oa.COMPONENT_ID = mc.COMPONENT_ID
        and o.order_id = oa.order_id
        and gc.causal_id(+) = o.causal_id
        and os.order_status_id = o.order_status_id
        and ot.TASK_TYPE_ID = o.TASK_TYPE_ID
        and oa.ORIGIN_ACTIVITY_ID is null
        and mc.COMPONENT_ID_PROD != inucomp;

        rcOrdenact      cuOrdenact%rowtype;


        
    BEGIN
        nuServicio  := null;
        nuComponente := null;
        
        sbHash := tbRegistro.first;
        if sbHash is null then
            sbComentario := 'Error 1.1|'||nuServicio||'|'||nuComponente||
            '|Sin Productos para validación|NA';
            raise raise_continuar;
        end if;
        
        loop
            begin
                nuServicio  := tbRegistro(sbHash).sesunuse;
                nuComponente := tbRegistro(sbHash).cmssidco;
                
                rcServicio := null;
                open cuServicio(nuServicio);
                fetch cuServicio into rcServicio;
                close cuServicio;

                if rcServicio.sesunuse is null then
                    sbComentario := 'Error 1.2|'||nuServicio||'|'||nuComponente||
                    '|No existe el producto en la BD|NA';
                    raise raise_continuar;
                end if;

                rcElemento := null;
                open cuElemento(nuServicio,nuComponente);
                fetch cuElemento into rcElemento;
                close cuElemento;

                if rcElemento.emsscoem is null then
                    sbComentario := 'Wrng 1.1|'||nuServicio||'|'||nuComponente||
                    '|No se encuentra identificador para el servicio ['||rcServicio.sesuserv||' - '||rcServicio.servdesc||']|NA';
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    nuWrng := nuWrng + 1;   
                end if;

                rcComponente := null;
                open cuComponente(nuServicio,nuComponente);
                fetch cuComponente into rcComponente;
                close cuComponente;

                if rcComponente.cmsssesu is null then
                    sbComentario := 'Error 1.3|'||nuServicio||'|'||nuComponente||
                    '|No existe el  componente para el servicio en la BD|NA';
                    raise raise_continuar;
                elsif rcComponente.cmssescm != 9 then 
                    sbComentario := 'Error 1.4|'||nuServicio||'|'||nuComponente||
                    '|El componente ya no se encuentra retirado ['||rcComponente.cmssescm||' - '||rcComponente.description||']|NA';
                    raise raise_continuar;
                end if;

                rcSolicitud := null;
                nuContador := 0;
                for rc in cuSolicitud (nuServicio) loop
                    nuContador := nuContador + 1;
                    rcSolicitud := rc;
                end loop;

                if nuContador = 0 then
                    sbComentario := 'Wrng 1.2|'||nuServicio||'|'||nuComponente||
                    '|Producto sin ordenes o peticiones pendientes|NA';
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    nuWrng := nuWrng + 1;   
                elsif rcSolicitud.cantidad < 2 then 
                    sbComentario := 'Wrng 1.3|'||nuServicio||'|'||nuComponente||
                    '|Producto con una unica orden ['||rcSolicitud.package_id||']['||rcSolicitud.cantidad||']|NA';
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    nuWrng := nuWrng + 1; 
                elsif nuContador > 1 then
                    sbComentario := 'Wrng 1.4|'||nuServicio||'|'||nuComponente||
                    '|Producto con más de una solicitud con ordenes pendientes ['||nuContador||']|NA';
                    rcSolicitud := null;
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    nuWrng := nuWrng + 1; 
                end if;
                  
                tbRegistro(sbHash).emsscoem := rcElemento.emsscoem;
                tbRegistro(sbHash).susccodi := rcServicio.susccodi;
                tbRegistro(sbHash).subscriber := rcServicio.subscriber_name;
                tbRegistro(sbHash).identidad := rcServicio.identification; 
                tbRegistro(sbHash).sesuserv := rcServicio.sesuserv; 
                tbRegistro(sbHash).servdesc := rcServicio.servdesc;   
                tbRegistro(sbHash).sesucicl := rcServicio.sesucicl;    
                tbRegistro(sbHash).sesucico := rcServicio.sesucico;    
                tbRegistro(sbHash).sesuesco := rcServicio.sesuesco;  
                tbRegistro(sbHash).escodesc := rcServicio.escodesc;  
                tbRegistro(sbHash).sesufein := rcServicio.sesufein;    
                tbRegistro(sbHash).sesufere := rcServicio.sesufere;    
                tbRegistro(sbHash).sesufucb := rcServicio.sesufucb;    
                tbRegistro(sbHash).sesucate := rcServicio.sesucate;    
                tbRegistro(sbHash).sesusuca := rcServicio.sesusuca;    
                tbRegistro(sbHash).sesudepa := rcServicio.sesudepa;    
                tbRegistro(sbHash).sesuloca := rcServicio.sesuloca;    
                tbRegistro(sbHash).sesuplfa := rcServicio.sesuplfa; 
                tbRegistro(sbHash).commerplan := rcServicio.commerplan;   
                tbRegistro(sbHash).addressp := rcServicio.addressp;
                tbRegistro(sbHash).addressc := rcServicio.addressc;
                tbRegistro(sbHash).cmssidcp := rcComponente.cmssidcp;
                tbRegistro(sbHash).cmssescm := rcComponente.cmssescm;
                tbRegistro(sbHash).escmdesc := rcComponente.escmdesc;
                tbRegistro(sbHash).cmsstcom := rcComponente.cmsstcom;
                tbRegistro(sbHash).tcomdesc := rcComponente.tcomdesc;
                tbRegistro(sbHash).cmssclse := rcComponente.cmssclse;
                tbRegistro(sbHash).clsedesc := rcComponente.clsedesc;
                tbRegistro(sbHash).cmssfein := rcComponente.cmssfein;
                tbRegistro(sbHash).cmssfere := rcComponente.cmssfere;
                tbRegistro(sbHash).cmsscouc := rcComponente.cmsscouc;
                tbRegistro(sbHash).cstatusid    := rcComponente.component_status_id;
                tbRegistro(sbHash).cstatusid_n  := rcComponente.cmssescm;
                tbRegistro(sbHash).sbActualiza  := 'N';
                tbRegistro(sbHash).packageid    := rcSolicitud.package_id;
                tbRegistro(sbHash).motiveid     := rcSolicitud.motive_id;
                tbRegistro(sbHash).pstatusid    := rcSolicitud.motive_status_id;
                tbRegistro(sbHash).pstatusdesc  := rcSolicitud.pstatusdesc;
                tbRegistro(sbHash).pstatusfin   := rcSolicitud.is_final_status;
                tbRegistro(sbHash).prqstdate    := rcSolicitud.request_date;
                tbRegistro(sbHash).ptypeid      := rcSolicitud.package_type_id;
                tbRegistro(sbHash).PtypeDesc    := rcSolicitud.ptypedesc;
                tbRegistro(sbHash).ptagname     := rcSolicitud.tag_name;
                tbRegistro(sbHash).activityid   := rcSolicitud.activity_id;
                tbRegistro(sbHash).instanceid   := rcSolicitud.instance_id;
                tbRegistro(sbHash).cantorden    := rcSolicitud.cantidad;

                if rcSolicitud.package_id is not null then
                    rcOrdenret := null;
                    nuHash := null;
                    open cuOrdenret(rcSolicitud.motive_id,nuComponente);
                    fetch cuOrdenret into rcOrdenret;
                    close cuOrdenret;
                    
                    if rcOrdenret.order_id is not null then
                        nuHash := 0;
                        tbRegistro(sbHash).tbOrdenes(nuHash).orderid            := rcOrdenret.order_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusid          := rcOrdenret.ostatusid; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusid_n        := 12;
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusdesc        := rcOrdenret.ostatusdesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusfin         := rcOrdenret.ostatusfin; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).causalid           := rcOrdenret.causal_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).causaldesc         := rcOrdenret.causaldesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).classcausalid      := rcOrdenret.class_causal_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).classcausaldesc    := rcOrdenret.classcausaldesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).createddate        := rcOrdenret.created_date; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).assigneddate       := rcOrdenret.assigned_date; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).legalizadate       := rcOrdenret.legalization_date; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).orderactivityid    := rcOrdenret.order_activity_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).status             := rcOrdenret.status; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).status_n           := 'F';
                        tbRegistro(sbHash).tbOrdenes(nuHash).tasktypeid         := rcOrdenret.task_type_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).tasktypedesc       := rcOrdenret.tasktypedesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).componentid        := rcOrdenret.component_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).componentprod      := rcOrdenret.component_id_prod; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).sbActualiza        := 'N'; 
                        
                        if rcSolicitud.cantidad = 1 then
                            tbRegistro(sbHash).tbOrdenes(nuHash).ostatusid_n    := rcOrdenret.ostatusid;
                            tbRegistro(sbHash).tbOrdenes(nuHash).status_n       := rcOrdenret.status;        
                        end if;
                    end if;

                    nuHash := 1;
                    for rcOrdenact in cuOrdenact(rcSolicitud.motive_id,nuComponente) loop
                        tbRegistro(sbHash).tbOrdenes(nuHash).orderid            := rcOrdenact.order_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusid          := rcOrdenact.ostatusid; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusid_n        := rcOrdenact.ostatusid;
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusdesc        := rcOrdenact.ostatusdesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).ostatusfin         := rcOrdenact.ostatusfin; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).causalid           := rcOrdenact.causal_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).causaldesc         := rcOrdenact.causaldesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).classcausalid      := rcOrdenact.class_causal_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).classcausaldesc    := rcOrdenact.classcausaldesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).createddate        := rcOrdenact.created_date; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).assigneddate       := rcOrdenact.assigned_date; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).legalizadate       := rcOrdenact.legalization_date; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).orderactivityid    := rcOrdenact.order_activity_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).status             := rcOrdenact.status;
                        tbRegistro(sbHash).tbOrdenes(nuHash).status_n           := rcOrdenact.status;
                        tbRegistro(sbHash).tbOrdenes(nuHash).tasktypeid         := rcOrdenact.task_type_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).tasktypedesc       := rcOrdenact.tasktypedesc; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).componentid        := rcOrdenact.component_id; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).componentprod      := rcOrdenact.component_id_prod; 
                        tbRegistro(sbHash).tbOrdenes(nuHash).sbActualiza        := 'N'; 
                        nuHash := nuHash + 1;    
                    end loop;
                end if;

                pActualizaDatos(tbRegistro(sbHash)); 
                pGeneraciondeRastro(tbRegistro(sbHash)); 
                tbRegistro.delete(sbHash);
                

            exception
                when raise_continuar then
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    tbRegistro.delete(sbHash);
                    nuErr := nuErr + 1;  
                when others then
                    sbErrorMensaje := sqlerrm;
                    --pkerrors.geterrorvar (nuErrorCode, sbErrorMensaje);
                    sbComentario := 'Error 1.0|'||nuServicio||'|'||nuComponente||
                    '|Error desconocido en analisis de datos|'||sbErrorMensaje;
                    pEscritura(tbArchivos(cnuIdErr),sbComentario);
                    tbRegistro.delete(sbHash);
                    nuErr := nuErr + 1;  
                    
                    if cuServicio%isopen then
                        close cuServicio;
                    end if;
                    
                    if cuElemento%isopen then
                        close cuElemento;
                    end if; 

                    if cuComponente%isopen then
                        close cuComponente;
                    end if; 

                    if cuSolicitud%isopen then
                        close cuSolicitud;
                    end if; 

                    if cuOrdenret%isopen then
                        close cuOrdenret;
                    end if; 

                    if cuOrdenact%isopen then
                        close cuOrdenact;
                    end if;                    
            end;

            sbHash := tbRegistro.next(sbHash);
            exit when sbHash is null;    
        end loop;

       
    exception
        when raise_continuar then
            pEscritura(tbArchivos(cnuIdErr),sbComentario);
            nuErr := nuErr + 1;  
    end pAnalizaDatos;
    
      
    PROCEDURE pLecturaDatos IS
        cursor cuValida (inucomp in compsesu.cmssidco%type) is
        select component_status_id,cmssescm,component_id,product_id 
        from pr_component,compsesu 
        where component_id = inucomp
        and component_id = cmssidco
        and product_id = cmsssesu;

        rcValida    cuValida%rowtype;

    BEGIN
        nuServicio  := null;
        nuComponente := null;

        if csbdOutPut = 'S' or tbArchivos(0).flgprint = 'N' then
            --nuPivote := 1;
            open cuLecManual;
            loop
                tbLecManual.delete;
                fetch cuLecManual bulk collect into tbLecManual limit cnuLimit;
                exit when tbLecManual.count = 0; 

                for i in 1..tbLecManual.count loop 
                    begin
                        nuLine := nuLine+ 1;
                        nuServicio := tbLecManual(i).sesunuse;
                        nuComponente := tbLecManual(i).cmssidco;
                        
                        sbHash := lpad(nuServicio,cnuHash,'0');
                        if not tbRegistro.exists(sbHash) then
        
                            tbRegistro(sbHash).sesunuse := nuServicio;
                            tbRegistro(sbHash).cmssidco := nuComponente;
                            tbRegistro(sbHash).sbFlag := 'N';
                            
                            nuTotal := nuTotal + 1;

                        else
                            sbComentario := 'Error 0.2|'||nuServicio||'|'||nuComponente||
                            '|Servicio duplicado|NA';
                            tbRegistro(sbHash).sbFlag := 'S';
                            pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                            nuErr := nuErr + 1;
                        end if; 

                    exception
                        when others then
                            sbErrorMensaje := sqlerrm;
                            --pkerrors.geterrorvar (nuErrorCode, sbErrorMensaje);
                            sbComentario := 'Error 0.0|'||nuServicio||'|'||nuComponente||
                            '|Error desconocido en generación de datos de entrada|'||sbErrorMensaje;
                            pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                            nuErr := nuErr + 1;
                            tbRegistro(sbHash).sbFlag := 'S';
                    end;
                end loop; 

                --nuPivote := tbLecManual(tbLecManual.last).sesunuse;
                exit when tbLecManual.count < cnuLimit; 
            end loop;
            close cuLecManual;
        else
            loop
                begin
                
                    osbLine := null;
                    if fgetnextline (tbArchivos(0).flFile, osbline) then 
                        exit; 
                    end if;
                    
                    tbCampos.delete;
                    parsestring(osbline, csbPIPE, tbCampos);
                    
                    if tbCampos.exists(1) and tbCampos.exists(2) then
                        
                        nuServicio := tbCampos(1);
                        nuComponente := tbCampos(2);
                        
                        sbHash := lpad(nuServicio,cnuHash,'0');
                        nuLine := nuLine + 1;
                        
                        if not tbRegistro.exists(sbHash) then
                        
                            tbRegistro(sbHash).sesunuse := nuServicio;
                            tbRegistro(sbHash).cmssidco := nuComponente;
                            tbRegistro(sbHash).sbFlag := 'N';
                            
                            nuTotal := nuTotal + 1;                           
                        else
                            sbComentario := 'Error 0.2|'||nuServicio||'|'||nuComponente||
                            '|Servicio duplicado|NA';
                            tbRegistro(sbHash).sbFlag := 'S';
                            raise raise_continuar;
                        end if;
                    else
                        sbComentario := 'Error 0.1|'||nuServicio||'|'||nuComponente||
                        '|Los datos de entrada no pueden ser nulos ['||osbline||']|NA';
                        raise raise_continuar;    
                    end if;
                exception
                    when raise_continuar then
                        pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                        nuErr := nuErr + 1; 
                    when others then
                        sbErrorMensaje := sqlerrm;
                        --pkerrors.geterrorvar (nuErrorCode, sbErrorMensaje);
                        sbComentario := 'Error 0.0|'||nuServicio||'|'||nuComponente||
                        'Error desconocido en lectura de archivo de entrada ['||osbline||']|'||sbErrorMensaje;
                        pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                        nuErr := nuErr + 1;  
                        tbRegistro(sbHash).sbFlag := 'S';
                end;
            end loop;
        end if;
        
        sbHash := tbRegistro.first;
        if sbHash is not null then
            loop
                if tbRegistro(sbHash).sbFlag = 'S' then
                    tbRegistro.delete(sbHash);
                    nuTotal := nuTotal - 1;
                else

                    nuServicio := tbRegistro(sbHash).sesunuse;
                    nuComponente := tbRegistro(sbHash).cmssidco;

                    rcValida := null;
                    open cuValida (nuComponente);
                    fetch cuValida into rcValida;
                    close cuValida;

                    if rcValida.component_id is null then
                        sbComentario := 'Error 0.3|'||nuServicio||'|'||nuComponente||
                        '|No existe el componente en la base de datos|NA';
                        pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                        nuTotal := nuTotal - 1;
                        nuErr := nuErr + 1;
                        tbRegistro.delete(sbHash);
                    elsif rcValida.product_id != nuServicio then
                        sbComentario := 'Error 0.4|'||nuServicio||'|'||nuComponente||
                        '|El producto del componente no corresponde con el esperado ['||rcValida.product_id||']|NA';
                        pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                        nuTotal := nuTotal - 1;
                        nuErr := nuErr + 1;
                        tbRegistro.delete(sbHash);
                    elsif rcValida.component_status_id = rcValida.cmssescm then
                        sbComentario := 'Error 0.5|'||nuServicio||'|'||nuComponente||
                        '|El estado del componente ya se encuentra sincronizado ['||rcValida.component_status_id||']|NA';
                        pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                        nuTotal := nuTotal - 1;
                        nuErr := nuErr + 1;
                        tbRegistro.delete(sbHash);
                    /*elsif rcValida.component_status_id = 9 then
                        sbComentario := 'Error 0.5|'||nuServicio||'|'||nuComponente||
                        '|El estado del componente ya se encuentra retirado ['||rcValida.component_status_id||']|NA';
                        pEscritura(tbArchivos(cnuIdErr),sbComentario); 
                        nuTotal := nuTotal - 1;
                        nuErr := nuErr + 1;
                        tbRegistro.delete(sbHash);
                    elsif rcValida.component_status_id != 5 then 
                        sbComentario := 'Error 0.6|'||nuServicio||'|'||nuComponente||
                        '|El estado del componente es diferente del esperado ['||rcValida.component_status_id||' - '||5||']|NA';
                        pEscritura(tbArchivos(ccnuIdErr),sbComentario); 
                        nuTotal := nuTotal - 1;
                        nuErr := nuErr + 1;
                        tbRegistro.delete(sbHash);*/
                    end if;
                end if;
                sbHash := tbRegistro.next(sbHash);
                exit when sbHash is null;
            end loop;
        end if;

    exception
        when others then
            nuErr := nuErr + 1;
            raise;
    END pLecturaDatos;
    
begin
    pInicializar(); 
    pAbrirArchivo(); 
    pLecturaDatos(); 
    pAnalizaDatos(); 
    pCerrarArchivo(); 
    
exception
    when others then
        pCerrarArchivoE();
end;
