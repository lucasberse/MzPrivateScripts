process.env.GOOGLE_APPLICATION_CREDENTIALS = "./gcp-key.json";
require('dotenv').config(); 
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const axios = require('axios');
const xml2js = require('xml2js');
const { BigQuery } = require('@google-cloud/bigquery');

// Configurar
const projectId = 'managerzone-456020';
const datasetId = 'managerzone_db';
const bigquery = new BigQuery({ projectId });
const matchesTableId = 'matches';
const playersTableId = 'players';
const statsTableId = 'player_stats';
puppeteer.use(StealthPlugin());

const TEAM_ID = '1099103'; // ID del equipo

async function fetchMatchIds() {
    const url = 'http://www.managerzone.com/xml/team_matchlist.php?sport_id=1&team_id=1099103&match_status=1&limit=50';
    const response = await axios.get(url);
    const xml = response.data;

    const parser = new xml2js.Parser();
    return new Promise((resolve, reject) => {
        parser.parseString(xml, (err, result) => {
            if (err) return reject(err);
            
            // Extraer los 'id' de los partidos
            const matchIds = result.ManagerZone_MatchList.Match.map(match => match.$.id);
            resolve(matchIds);
        });
    });
}

function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

async function fetchMatchType(matchId, xmlResponse) {
    const xml = xmlResponse.data;
    const parser = new xml2js.Parser();
    return new Promise((resolve, reject) => {
        parser.parseString(xml, (err, result) => {
            if (err) {
                return reject(err);
            }
            
            // Asegurar que existe la lista de partidos
            if (!result.ManagerZone_MatchList || !result.ManagerZone_MatchList.Match) {
                return resolve({ matchType: 'Desconocido', typeId: 'Desconocido' });
            }
            
            const matches = result.ManagerZone_MatchList.Match;
            
            for (const match of matches) {
                if (parseInt(match.$.id) == matchId) {
                    resolve({
                        matchType: match.$.type,  // Obtener el tipo de partido
                        typeId: match.$.typeId   // Obtener el typeId
                    });
                    return;
                }
            }
            resolve({ matchType: 'Desconocido', typeId: 'Desconocido' });
        });
    });
}



async function scrapeMatchData(matchId) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });

    await page.goto(`https://www.managerzone.com/?p=match&sub=result&mid=${matchId}`, { waitUntil: 'domcontentloaded' });
    // wait for selector for 10 seconds and if not skip
    try {
        await page.waitForSelector('#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll', { visible: true, timeout: 10000  });
        await page.click('#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll');
    } catch (error) {
        console.log(' no encontrado, continuando...');
    }

    // Esperar y hacer clic en la pestaña de estadísticas
    await page.waitForSelector('#ui-id-3');
    await page.click('#ui-id-3');

    //screenshot
    // Esperar que cargue la pestaña
    await delay(2000);
    
    // Activar estadísticas detalladas
    await page.waitForSelector('#detailedToggle .handle');
    while (!(await page.$('#detailedToggle .handle.on'))) {
        await page.click('#detailedToggle');
        await delay(1000);
    }
    
    // Obtener la tabla de estadísticas detalladas
    const stats = await page.evaluate((matchId) => {
        const myTeamId = 1099103;
        const matchIdLocal = matchId;
        // Buscar los elementos <a> con href que contengan /?p=team&amp;tid={id_team}
        const teamLinks = Array.from(document.querySelectorAll('a[href*="p=team&tid="]'));

        let rivalName = 'Desconocido';
        for (const link of teamLinks) {
            const href = link.getAttribute('href');
            const match = href.match(/tid=(\d+)/);
            if (match) {
                const teamId = parseInt(match[1]);
                if (teamId !== myTeamId) {
                    rivalName = link.innerText.trim();  // Nombre del rival
                    break;
                }
            }
        }
        const headings = Array.from(document.querySelectorAll('h2'));
        let targetDiv = null;
        for (const h2 of headings) {
            if (h2.textContent.trim() === '★ Lobos FC ★') {
                targetDiv = h2.nextElementSibling; // Buscar el div después del h2
                break;
            }
        }
        const rows = targetDiv.querySelectorAll('.hitlist.statsLite.matchStats.matchStats--detailed tbody tr.odd, tr.even');
        let data = [];
        let positions = [];
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 19) {
                const playerLink = cells[0].querySelector('a');
                const playerId = playerLink ? playerLink.href.match(/pid=(\d+)/)[1] : null;
                const playerName = playerLink ? playerLink.innerText.trim() : null;
                const position = cells[1].innerText.trim();
                const minutesPlayed = parseInt(cells[2].innerText.replace("'", "").trim());
                if (minutesPlayed === 0) return; // Omitir jugadores sin minutos
                
                if (positions.length < 11 && position !== 'Ar') {
                    positions.push(position);
                }
                
                const goals = parseInt(cells[3].innerText.trim());
                const assists = parseInt(cells[4].innerText.trim());
                const shotsTotal = parseInt(cells[5].innerText.trim());
                const shotGoalRate = parseInt(cells[6].innerText.replace('%', '').trim());
                const shotsOnTarget = parseInt(cells[7].innerText.trim());
                const shotOnTargetRate = parseInt(cells[8].innerText.replace('%', '').trim());
                const passesTotal = parseInt(cells[10].innerText.trim());
                const passesSuccessful = parseInt(cells[11].innerText.trim());
                const passesFailed = parseInt(cells[12].innerText.trim());
                const passesSuccessRate = parseInt(cells[13].innerText.replace('%', '').trim());
                const interceptions = parseInt(cells[15].innerText.trim());
                const tacklesTotal = parseInt(cells[16].innerText.trim());
                const tacklesSuccessful = parseInt(cells[17].innerText.trim());
                const tacklesFailed = parseInt(cells[18].innerText.trim());
                const tacklesSuccessRate = parseInt(cells[19].innerText.replace('%', '').trim());
                
                data.push({
                    match_id: parseInt(matchIdLocal),
                    player_id: parseInt(playerId),
                    player_name: playerName,
                    position,
                    minutes_played: minutesPlayed,
                    goals,
                    assists,
                    shots_total: shotsTotal,
                    shot_goal_rate: shotGoalRate,
                    shots_on_target: shotsOnTarget,
                    shot_on_target_rate: shotOnTargetRate,
                    passes_total: passesTotal,
                    passes_successful: passesSuccessful,
                    passes_failed: passesFailed,
                    passes_success_rate: passesSuccessRate,
                    interceptions,
                    tackles_total: tacklesTotal,
                    tackles_successful: tacklesSuccessful,
                    tackles_failed: tacklesFailed,
                    tackles_success_rate: tacklesSuccessRate
                });
            }
        });


        return { data, positions, rivalName };
    });
    
    await browser.close();
    return stats;
}

function determineTactic(positions) {
    const counts = { De: 0, Me: 0, At: 0 };
    let total = 0;

    for (const pos of positions) {
        if (counts[pos] !== undefined) {
            counts[pos]++;
            total++;

            if (total === 10) break; // Detener cuando el total llegue a 10
        }
    }

    return `${counts.De}-${counts.Me}-${counts.At}`;
}

async function saveToBigQuery(matchId, data, positions, rivalName, xmlMatchesResponse) {
    const tactic = determineTactic(positions);
    const { matchType, typeId } = await fetchMatchType(matchId, xmlMatchesResponse);

    // 1. Insertar partido si no existe
    const [matches] = await bigquery.query({
        query: `SELECT id FROM \`${projectId}.${datasetId}.${matchesTableId}\` WHERE id = @matchId`,
        parameterMode: 'named',
        params: { matchId },
    });
    

    if (matches.length === 0) {
        await bigquery.dataset(datasetId).table(matchesTableId).insert([{
            id: matchId,
            tactic,
            type: matchType,
            match_type_id: parseInt(typeId),
            rival: rivalName,
        }]);
    }

    // 2. Insertar nuevos jugadores si no existen
    const uniquePlayerIds = [...new Set(data.map(p => p.player_id))];
    const [existingPlayers] = await bigquery.query({
        query: `SELECT id FROM \`${projectId}.${datasetId}.${playersTableId}\` WHERE id IN UNNEST(@ids)`,
        params: { ids: uniquePlayerIds },
        parameterMode: 'named'
    });

    const existingPlayerIds = new Set(existingPlayers.map(p => p.id));

    const newPlayers = data
        .filter(player => !existingPlayerIds.has(player.player_id))
        .map(player => ({ id: player.player_id, name: player.player_name }));

    if (newPlayers.length > 0) {
        await bigquery.dataset(datasetId).table(playersTableId).insert(newPlayers);
    }

    // 3. Insertar estadísticas evitando duplicados (match_id + player_id)
    const statsData = data.map(({ player_name, ...rest }) => ({
        ...rest,
        match_id: matchId,
    }));

    // Obtener combinaciones existentes para este match
    const [existingStats] = await bigquery.query({
        query: `SELECT player_id FROM \`${projectId}.${datasetId}.${statsTableId}\` WHERE match_id = @matchId`,
        params: { matchId },
        parameterMode: 'named'
    });

    const existingPlayerStats = new Set(existingStats.map(stat => stat.player_id));

    // Filtrar solo los que no están ya insertados
    const filteredStatsData = statsData.filter(stat => !existingPlayerStats.has(stat.player_id));

    if (filteredStatsData.length > 0) {
        try {
            await bigquery.dataset(datasetId).table(statsTableId).insert(filteredStatsData);
            console.log(`✅ Insertadas ${filteredStatsData.length} stats para match ${matchId}.`);
        } catch (err) {
            console.error('❌ Error al insertar en BigQuery:', err);
        }
    } else {
        console.log(`⏩ Todas las stats del partido ${matchId} ya existen. No se insertó nada.`);
    }

}


(async () => {
    const matchIds = await fetchMatchIds();
    console.log(matchIds); // Verifica que los IDs se extraen correctamente
    const url = `http://www.managerzone.com/xml/team_matchlist.php?sport_id=1&team_id=${TEAM_ID}&match_status=1&limit=100`;
    const xmlMatchesResponse = await axios.get(url);
    for (const matchId of matchIds) {
        const numericMatchId = parseInt(matchId);
        // Verificar si el match ya tiene stats de jugadores en BigQuery
        const [existingStats] = await bigquery.query({
            query: `SELECT player_id FROM \`${projectId}.${datasetId}.${statsTableId}\` WHERE match_id = @matchId LIMIT 1`,
            parameterMode: 'named',
            params: {
                matchId: numericMatchId,
            },
        });
        
        
        if (existingStats.length > 0) {
            console.log(`⏩ Partido ${matchId} ya tiene stats en BigQuery. Saltando...`);
            continue;
        }

        const { data, positions, rivalName } = await scrapeMatchData(matchId);
        //print all the data for each match to check if it is correct
        console.log(`Match ID: ${matchId}`);
        console.log('Data:', data);
        console.log('Positions:', positions);
        console.log('Rival:', rivalName);
        // Guardar en bigquery
        await saveToBigQuery(numericMatchId, data, positions, rivalName, xmlMatchesResponse);
    }
})();