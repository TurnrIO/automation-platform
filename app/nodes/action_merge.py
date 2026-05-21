"""Merge / join action node.
"""
import logging

logger = logging.getLogger(__name__)

NODE_TYPE = "action.merge"
LABEL = "Merge / Join"


def run(config, inp, context, logger, creds=None, **kwargs):
    """
    Merge outputs from multiple upstream nodes.
    mode=first : pass through first upstream value (default)
    mode=all   : collect all upstream node outputs into a list
    mode=dict  : merge all upstream dicts into one dict (last wins on collision)
    """
    logger.info("Merge: starting")
    mode = config.get('mode', 'dict')
    upstream_ids = kwargs.get('upstream_ids', [])
    upstream_outputs = [context.get(uid) for uid in upstream_ids if context.get(uid) is not None]

    if mode == 'first' or not upstream_ids:
        logger.info("Merge: mode=first, passed through input")
        return inp

    if mode == 'all':
        result = {'merged': upstream_outputs, 'count': len(upstream_outputs)}
        logger.info("Merge: mode=all, merged %s outputs → %s", len(upstream_outputs), result)
        return result

    elif mode == 'dict':
        result = {}
        for out in upstream_outputs:
            if isinstance(out, dict):
                result.update(out)
        logger.info("Merge: mode=dict, merged %s outputs → %s keys", len(upstream_outputs), len(result))
        return result

    else:
        logger.info("Merge: unknown mode=%s, passed through input", mode)
        return inp
