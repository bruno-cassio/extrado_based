<----->

    ------------>Junto, Yellum, Porto e HDi. 

< Base referencia atender tanto geração de dados Conta Virtual quanto Manutenção de inserção dados CSCs; >

Construção das bases de referencia Caller do CoreMain 
    --> Competencias, Cias, Corretora, Base Referencia Origem relatorio correspondete de cada CIA; EM processes file devemos considerar a buisca da raiz da cia em Numeros. 

Preparação de ambiente para execução Submit assumir variabilidade e combinação das referencias bases na extração
    --> Validar a consulta e geração atual de valor CIA, Raiz Relatório exec;;

--> COMPETENCIAS OPTS, PADRONIZAÇÃO INICIAL CONSIDERANDO SOMENTE 2025


Refatoração do caller inicial CoreMain para flexibilização do reaproveitamento futuro na entrega de UI. Originalmente o Core tratava da execução baseada em call do extrator ou
preset. Criada leitura de multiplas competencias e multiplas cias para execução do Core, com objetivo de atender a UI na função de SUBMIT para periodos, cias diferentes independente
da solução escolhida futuramente. 

Construção das bases de referencias origens de Arquivos de Leitura correspondentes às CIAS e respctivas competencias de execução, dinamicamente, considerando a atribuição de MesAno,


    >> Análise Porto

        Unificar as abas, considerando ser o mesmo cabeçalho; 
        Somar Ganho GC para chegar no total relatório;
        Fator Melchiori, CAixa Liq / Soma Ganho GC
        Premio REC, coluna 'Produção Emitida atual - Para Pgto' * FatorMelchiori
        CV * 1% PremioREC
        VI * 1% PremioREC



--> Criar validação a respeito de corretoras não mapeadas no inicio do processo
   -- tabela de logs de cada execução de batch runner ? 
   -- emissão relatório de inconsistencias, consolidando corretoras nao mapeadas e cias sem relatório / valor caixa; 
    --- já existe a função que valida se as  corretoras selecionadas possuem conta virtual gerada, a partir dai alimentar o relatório de inconsistencia. 

    Calculo TOKIO

    calcular o premio base dividindo o valor total com pelo total com% , é o premio base
    gera premio rec em cima do premio base * 0,9850, 
    1 % é CV 
    e 0,3 % é VI

