require('dotenv').config(); 
const puppeteer = require('puppeteer');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const xml2js = require('xml2js');

// Configurar Supabase
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const matchIds = [1506387316]; // Lista de IDs de partidos
const TEAM_ID = '1099103'; // ID del equipo

function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

async function fetchMatchType(matchId) {
    const url = `http://www.managerzone.com/xml/team_matchlist.php?sport_id=1&team_id=${TEAM_ID}&match_status=1&limit=100`;
    const response = await axios.get(url);
    const xml = response.data;

    const parser = new xml2js.Parser();
    return new Promise((resolve, reject) => {
        parser.parseString(xml, (err, result) => {
            if (err) {
                return reject(err);
            }

            // Asegurar que existe la lista de partidos
            if (!result.ManagerZone_MatchList || !result.ManagerZone_MatchList.Match) {
                return resolve('Desconocido');
            }

            const matches = result.ManagerZone_MatchList.Match; // Corregido

            for (const match of matches) {
                if (parseInt(match.$.id) === matchId) {  // Acceder al atributo 'id' correctamente
                    resolve(match.$.type); // Acceder al atributo 'type' correctamente
                    return;
                }
            }
            resolve('Desconocido'); // Default type if not found
        });
    });
}


async function scrapeMatchData(matchId) {
    const browser = await puppeteer.launch({ headless: false });
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

async function saveToSupabase(matchId, data, positions, rivalName) {
    // Verificar si el partido ya existe en la base de datos
    let { data: matchExists } = await supabase.from('matches').select('*').eq('id', matchId);
    
    if (!matchExists || matchExists.length === 0) {
        const tactic = determineTactic(positions);
        const matchType = await fetchMatchType(matchId); // Obtener el tipo de partido
        await supabase.from('matches').insert([{ id: matchId, tactic, type: matchType, rival: rivalName }]);
    }
    
    // Recuperar IDs de jugadores existentes
    const { data: existingPlayers } = await supabase.from('players').select('id, name');
    const existingPlayerMap = new Map(existingPlayers.map(player => [player.id, player.name]));

    // Filtrar y agregar nuevos jugadores
    const newPlayers = data.filter(playerStat => !existingPlayerMap.has(playerStat.player_id));
    if (newPlayers.length > 0) {
        const playerInserts = newPlayers.map(player => ({
            id: player.player_id,
            name: player.player_name
        }));
        const { error: playerInsertError } = await supabase.from('players').insert(playerInserts);
        if (playerInsertError) console.error('Error insertando jugadores en Supabase:', playerInsertError);
    }

    // Eliminar player_name del objeto de estadísticas antes de la inserción
    const playerStatsWithMatchId = data.map(({ player_name, ...rest }) => ({
        ...rest,
        match_id: matchId  // Asegurarse de agregar el match_id
    }));

    // Insertar estadísticas de jugadores usando upsert para evitar duplicados
    const { error: statsInsertError } = await supabase.from('player_stats').upsert(playerStatsWithMatchId, {
        onConflict: ['match_id', 'player_id'] // Evitar duplicados de match_id y player_id
    });

    if (statsInsertError) {
        console.error('Error insertando estadísticas en Supabase:', statsInsertError);
    } else {
        console.log('Datos insertados correctamente.');
    }
}

(async () => {
    for (const matchId of matchIds) {
        const { data, positions, rivalName } = await scrapeMatchData(matchId);
        //print all the data for each match to check if it is correct
        console.log(`Match ID: ${matchId}`);
        console.log('Data:', data);
        console.log('Positions:', positions);
        console.log('Rival:', rivalName);
        // Guardar en Supabase
        await saveToSupabase(matchId, data, positions, rivalName);
    }
})();