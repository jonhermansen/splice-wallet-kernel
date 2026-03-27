// Copyright (c) 2025-2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { Request, Response, NextFunction } from 'express'
import { AuthAware } from '@canton-network/core-wallet-auth'
import { Logger } from 'pino'
import { Store } from '@canton-network/core-wallet-store'

/**
 * Middleware to handle session validation based on user sessions.
 * @param store needs to be AuthAware
 * @param allowedPaths a record of path -> list of methods which do not require authentication
 * @param logger
 * @returns
 */
export function sessionHandler(
    store: Store & AuthAware<Store>,
    allowedPaths: Record<string, string[]>,
    logger: Logger
) {
    return async (req: Request, res: Response, next: NextFunction) => {
        const context = req.authContext

        // Use baseUrl + path instead of just baseUrl to determine the full route.
        // The middleware is mounted with app.use('/api/*splat', ...) in init.ts.
        // In path-to-regexp 8.4.0 (https://github.com/pillarjs/path-to-regexp/commit/43669ac),
        // "Dedupe regex prefixes (#422)" changed how wildcard splat patterns split
        // req.baseUrl and req.path:
        //   8.3.0: baseUrl="/api/v0/dapp"  path="/"
        //   8.4.0: baseUrl="/api/v0"       path="/dapp"
        // Using baseUrl + path is stable across both versions.
        const requestPath = (req.baseUrl + req.path).replace(/\/$/, '')
        const allowedMethods = allowedPaths[requestPath]

        if (req.method !== 'POST') {
            logger.debug(
                'Skipping authentication for OPTIONS request to ' + req.baseUrl
            )
            next()
        } else if (
            allowedMethods &&
            (allowedMethods.includes(req.body.method) ||
                allowedMethods.includes('*'))
        ) {
            logger.debug(
                `Allowing unauthenticated access to ${req.baseUrl} for method ${req.body.method}`
            )
            next()
        } else {
            logger.debug('Checking for active session for ' + context?.userId)
            const session = await store.withAuthContext(context).getSession()
            if (!session) {
                logger.debug('No active session found for ' + context?.userId)
                res.status(401).json({ error: 'No active session found' })
            } else {
                next()
            }
        }
    }
}
