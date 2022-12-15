import type {Config} from 'jest';

const config: Config = {
    moduleNameMapper: {
        '^\\S+.(css|styl|less|sass|scss|png|jpg|ttf|woff|woff2)$': 'jest-transform-stub',
    },
    modulePathIgnorePatterns: ['<rootDir>/tsc-check/'],
    preset: 'ts-jest',
    rootDir: '../../',
    setupFilesAfterEnv: [],
    testEnvironment: 'node',
    maxConcurrency: 1,
    maxWorkers: 1,
    testTimeout: 10e3,
    injectGlobals: false,
    bail: true, // stop after first failing test
    silent: true,
    passWithNoTests: true,
    errorOnDeprecated: true,
    collectCoverage: false
};

export default config;
