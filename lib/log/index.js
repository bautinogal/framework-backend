import env from '../../config/env.js';

const Reset = "\x1b[0m"
const Bright = "\x1b[1m"
const Dim = "\x1b[2m"
const Underscore = "\x1b[4m"
const Blink = "\x1b[5m"
const Reverse = "\x1b[7m"
const Hidden = "\x1b[8m"

const FgBlack = "\x1b[30m"
const FgRed = "\x1b[31m"
const FgGreen = "\x1b[32m"
const FgYellow = "\x1b[33m"
const FgBlue = "\x1b[34m"
const FgMagenta = "\x1b[35m"
const FgCyan = "\x1b[36m"
const FgWhite = "\x1b[37m"

const BgBlack = "\x1b[40m"
const BgRed = "\x1b[41m"
const BgGreen = "\x1b[42m"
const BgYellow = "\x1b[43m"
const BgBlue = "\x1b[44m"
const BgMagenta = "\x1b[45m"
const BgCyan = "\x1b[46m"
const BgWhite = "\x1b[47m"

export const log = (message="", before="", logLevel, options = {}) => {
    message = message && message.stack ? message.stack  : message;
    if(typeof message ===  'object') message = JSON.stringify(message, null, 4);
    if(logLevel > env.app.logLevel) console.log(`${before}${message}${Reset}`);
    return message;
};

export const fatal = (message, options) => log(message, `${Bright}${FgRed}[Fatal Error] `, 50000, options);
export const error = (message, options) => log(message, `${FgRed}[Error] `, 40000, options);
export const warn = (message, options) => log(message, `${FgYellow}[Warn] `, 30000, options);
export const info = (message, options) => log(message, `${Reset}[Info] `, 20000,options);
export const debug = (message, options) => log(message, `${FgWhite}[Debug]`, 10000, options);

export default {log, fatal, error, warn, info, debug}

