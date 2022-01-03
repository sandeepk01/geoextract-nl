const glob = require('glob');
const cmd = require('node-cmd');
const path = require('path');
const fs = require('fs-extra');
const workerpool = require('workerpool');
const chalk = require('chalk');
const log = console.log;
const { getFileName } = require('./helpers/utils');
const { openDB, closeDB, insertBulk } = require('./helpers/mongoUtils');
const chunk = require('lodash/chunk');

/// Configurable parameters
const numDataPath = path.resolve('C:\\NL_Extraction\\NL_Shapes'); /// Path where shape files are stored after extraction
const ogrPath = path.resolve('C:\\Program Files\\QGIS 3.16.9\\bin\\ogr2ogr.exe'); /// Path where QGIS binaries are installed
const pool = workerpool.pool(__dirname + '/helpers/geoJsonWorker.js', { maxWorkers: 6 }); /// Worker pool to process data before staging to MongoDB
const doProjection = true; // Set this to false if you want to shapefiles to be projected by this code instead of GDAL

// Run NUM Shape File conversions
async function numToJSON() {
    const start = Date.now();
    log(chalk.magentaBright('*************** STEP 1 - Converting NUMMER Shape files to GeoJSON ********************'));
    log(chalk.yellow(`STARTING : Please wait patiently as this is a ${chalk.underline('SERIAL')} process ....`));
    const files = glob.sync('nummer*.shp', { cwd: numDataPath });
    if (doProjection) {
        for (const i in files) {
            const cmdString = `"${ogrPath.toString()}" -s_srs EPSG:28992 -t_srs EPSG:4326 -f geoJSON "${numDataPath + path.sep + getFileName(files[i])}.json" "${
                numDataPath + path.sep + files[i]
            }" -emptyStrAsNull -skipfailures -relaxedFieldNameMatch`;
            cmd.runSync(cmdString);
            log(chalk.yellow(`Processed (${chalk.green(Number(i) + 1)} of ${files.length}): `, chalk.green(files[i])));
        }
    } else {
        for (const i in files) {
            const cmdString = `"${ogrPath.toString()}" -f geoJSON "${numDataPath + path.sep + getFileName(files[i])}.json" "${
                numDataPath + path.sep + files[i]
            }" -emptyStrAsNull -skipfailures -relaxedFieldNameMatch`;
            cmd.runSync(cmdString);
            log(chalk.yellow(`Processed (${chalk.green(Number(i) + 1)} of ${files.length}): `, chalk.green(files[i])));
        }
    }
    const total = Date.now() - start;
    log(chalk.green(`COMPLETED :  Took ${Math.round(total / 60000)} minutes.`));
    return 'COMPLETED';
}

// Read GEOJSON and create staging file
function stageForMongo() {
    return new Promise((resolve) => {
        const start = Date.now();
        log(chalk.magentaBright('*************** STEP 2 - Staging NUMMER GeoJSON files for MongoDB ********************'));
        log(chalk.yellow(`STARTING : Speeding up with ${chalk.underline('PARALLEL')} threads ....`));
        const woonGemMaster = JSON.parse(fs.readFileSync(__dirname + '/helpers/woonplaats-gemeente.json'));
        const files = glob.sync('nummer*.json', { cwd: numDataPath });
        let counter = 0;
        const failedFiles = [];
        for (const i in files) {
            pool.exec('processor', [numDataPath + path.sep + files[i], woonGemMaster, numDataPath + path.sep + `staging_${getFileName(files[i])}.json`, !doProjection])
                .then(() => {
                    counter++;
                    checkProgress();
                })
                .catch(() => {
                    failedFiles.push(files[i]);
                    counter++;
                    checkProgress();
                });
        }

        function checkProgress() {
            if (counter >= files.length) {
                pool.terminate();
                const total = Date.now() - start;
                log(chalk.green(`COMPLETED :  Took ${Math.round(total / 60000)} minutes.`));
                if (failedFiles.length) {
                    log(chalk.red('Failed files : ', failedFiles.join()));
                    resolve('FAIL');
                }
                resolve('SUCCESS');
            } else {
                log(chalk.blueBright(`Worker stats ::::  Active threads: ${chalk.green(pool.stats().busyWorkers)}    Pending tasks: ${chalk.yellow(pool.stats().pendingTasks)}`));
            }
        }
    });
}

async function insertIntoMongo() {
    const start = Date.now();
    log(chalk.magentaBright('*************** STEP 3 - Inserting NUMMER Staging files to MongoDB ********************'));
    log(chalk.yellow(`STARTING : Please wait patiently as this is a ${chalk.underline('SERIAL')} process ....`));
    await openDB();
    const files = glob.sync('staging*.json', { cwd: numDataPath });
    let counter = 0;
    for (const i in files) {
        const jsonData = JSON.parse(fs.readFileSync(numDataPath + path.sep + files[i]));
        for (const b of chunk(jsonData, 10000)) {
            await insertBulk(b, 'ADDRESS');
        }
        counter += jsonData.length;
        log(chalk.yellow(`Processed (${chalk.green(Number(i) + 1)} of ${files.length}): `, chalk.green(files[i])));
    }
    const total = Date.now() - start;
    log(chalk.green(`COMPLETED :  Took ${Math.round(total / 60000)} minutes.`));
    await closeDB();
    return counter;
}

try {
    (async function () {
        log(
            chalk.blueBright(`
###############################################################################################
                                Welcome to BAG Data Extractor
-----------------------------------------------------------------------------------------------
        
        `)
        );
        const start = Date.now();
        const res1 = await numToJSON();
        let res2 = null;
        let totalRec = null;
        if (res1 === 'COMPLETED') {
            res2 = await stageForMongo();
        }
        if (res2 === 'SUCCESS') {
            totalRec = await insertIntoMongo();
        }
        const total = Date.now() - start;
        log(
            chalk.blueBright(`

${chalk.green('TOTAL RECORDS in DATABASE : ', totalRec)}

${chalk.green(`ALL STAGES COMPLETED :  Took ${Math.round(total / 60000)} minutes.`)}

-----------------------------------------------------------------------------------------------
                            Enjoy analysing the extracted data!
###############################################################################################
        `)
        );
    })();
} catch (e) {
    log(chalk.red('EXTRACTOR ERROR :: ', e.message));
}
