const compact = require('lodash/compact');
const toUpper = require('lodash/toUpper');
const workerpool = require('workerpool');
const fs = require('fs-extra');
const { getFileName } = require('./utils');
const chalk = require('chalk');
const proj4 = require('proj4');

// Proj4 definition for EPSG:28992 Amersfoort / RD New system
// To be used if OGR is not able to convert it properly
const RDProj =
    '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +towgs84=565.2369,50.0087,465.658,-0.406857330322398,0.350732676542563,-1.8703473836068,4.0812 +no_defs';

// Main processor which is CPU intensive
async function geoJsonProcessor(filePath, gemWoonList, newFilePath, manualProjection) {
    function getFriendlyNames(dataList, woonplaats, gemeente) {
        if (woonplaats && gemeente) {
            const rec = dataList.find((w) => toUpper(w.town) === toUpper(woonplaats) && toUpper(w.municipal) === toUpper(gemeente));
            return rec ? rec : null;
        }
        return null;
    }

    try {
        if (filePath) {
            const jsonData = JSON.parse(fs.readFileSync(filePath));
            const features = jsonData['features'].filter(
                (d) => d['properties']['ONDERZOEK'] === 'N' && d['properties']['INACTIEF'] === 'N' && d['properties']['POSTCODE'] && !d['properties']['DATUM_EIND']
            );
            const list = [];
            features.forEach((d) => {
                const prop = d['properties'];
                const geo = d['geometry'];
                if (prop && geo) {
                    const substiteData = getFriendlyNames(gemWoonList, prop['WOONPLAATS'], prop['GEM_NAAM']);
                    const coords = manualProjection ? proj4(RDProj, 'WGS84', geo['coordinates']) : geo['coordinates'];
                    if (manualProjection) {
                        geo['coordinates'] = coords;
                    }
                    list.push({
                        _id: prop['NUMMER_ID'],
                        buildingNumber: toUpper(compact([prop['HUISNUMMER'], prop['HUISLETTER'], prop['TOEVOEGING']]).join('-')),
                        street: toUpper(prop['STRAATNAAM']),
                        postcode: toUpper(prop['POSTCODE']),
                        town: toUpper(substiteData ? substiteData.townCommon : prop['WOONPLAATS']),
                        latitude: coords[1],
                        longitude: coords[0],
                        municipality: toUpper(substiteData ? substiteData.municipalCommon : prop['WOONPLAATS']),
                        huisNummer: toUpper(prop['HUISNUMMER']),
                        huisLetter: toUpper(prop['HUISLETTER']),
                        toEvoeging: toUpper(prop['TOEVOEGING']),
                        straatNaam: prop['STRAATNAAM'],
                        woonplaats: prop['WOONPLAATS'],
                        gemeente: prop['GEM_NAAM'],
                        geometry: geo,
                    });
                }
            });
            fs.writeFileSync(newFilePath, JSON.stringify(list));
            console.log(chalk.yellow(`Processed ${chalk.green(list.length)} records in : ${chalk.green(getFileName(filePath))}`));
            return 'SUCCESS';
        }
    } catch (e) {
        console.log(chalk.red('Error occurred while processing file : '), getFileName(filePath), chalk.redBright(e.message));
        return 'FAILURE';
    }
}

// This is used to test the worker setup
async function workerTester(filePath, gemWoonList) {
    await new Promise((resolve) => {
        setTimeout(resolve, 3000);
    });
    return `Processed : ${getFileName(filePath)} while comparing with ${gemWoonList.length} references.`;
}

// create a worker and register public functions
workerpool.worker({
    processor: geoJsonProcessor,
    tester: workerTester,
});
