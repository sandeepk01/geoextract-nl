function datumStringToISODate(numStr) {
    if (numStr && numStr.length >= 8) {
        return [numStr.substring(0, 4), numStr.substring(4, 6), numStr.substring(6, 8)].join('-');
    }
    return null;
}

function getFileName(fullPath, woEtxn = true) {
    const name = fullPath.replace(/^.*[\\\/]/, '');
    if (woEtxn) {
        const splits = name.split('.');
        splits.pop();
        return splits.join('.');
    }
    return name;
}

function keys2English(key) {
    switch (key) {
        case 'Naamgeving ingetrokken':
        case 'Woonplaats ingetrokken':
            return 'WITHDRAWN';
        case 'Naamgeving uitgegeven':
        case 'Woonplaats aangewezen':
            return 'ISSUED';
        case 'Landschappelijk gebied':
            return 'LANDSCAPE';
        case 'Kunstwerk':
            return 'ARTWORK';
        case 'Spoorbaan':
            return 'RAILROAD';
        case 'Water':
            return 'WATER';
        case 'Terrein':
            return 'TERRAIN';
        case 'Administratief gebied':
            return 'ADMINISTRATIVE';
        case 'Weg':
            return 'AWAY';
        case 'Weg':
            return 'AWAY';
        case 'voorlopig':
            return 'UNCOMFIRMED';
        case 'definitief':
            return 'CONFIRMED';
        default:
            return 'NA';
    }
}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

module.exports = { datumStringToISODate, getFileName, keys2English, asyncForEach };
