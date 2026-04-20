# AI Handoff: Structured TZ vs Free-Text Routing Issue

## Goal

We have a Telegram bot that builds Google Sheets financial tables.

The end goal is:

1. Structured TZ input must work reliably.
2. Free-text input should also work, ideally via AI.
3. If free-text remains imperfect, structured TZ must still remain stable and production-safe.

Right now the highest priority is restoring and preserving the stable structured-TZ flow.

## Project Structure

Relevant files:

1. `src/telegram/webhookHandler.js`
   Main Telegram dialogue state machine and routing.

2. `src/telegram/tzParser.js`
   Parses structured TZ input from Telegram messages.

3. `src/ai/agentBrain.js`
   LLM integration for:
   - clarification generation
   - free-text parsing into TZ
   - free-text answer resolution
   - update payload generation

4. `src/google/appsScriptClient.js`
   Node client used to call the Apps Script web app.

5. `apps-script/FinancialReportsBuilder.gs`
   Google Apps Script backend that creates/updates/validates Google Sheets files.

## Stable Historical Baseline

The flow was previously stable before free-text expansion.

Likely stable commits:

1. `521702d`
2. `2bb07ff`

Those versions were good at handling structured TZ / formatted TZ-like input.

At that point:

1. Structured input went through a deterministic parser.
2. Clarification flow was simpler.
3. Table building worked reliably.
4. The bot did not try to route the same input through too many modes.

## Current Problem

After adding free-text support, the Telegram flow became a mixture of multiple concepts inside one pipeline:

1. Structured TZ parsing.
2. TZ-like plain text parsing.
3. Heuristic free-text parsing.
4. AI-based free-text parsing.
5. Custom architecture mode.
6. Editing-mode escape logic.
7. AI clarification generation.
8. Rule-based short-answer interpretation.
9. AI-based short-answer interpretation.

As a result, the stable structured-TZ path stopped being isolated.

That means a correctly formatted structured TZ can still end up interacting with logic that was introduced for free-text handling.

## Desired Product Behavior

There are two input modes and they must be intentionally separated.

### Mode 1: Structured TZ

Examples:

```text
business_name: Логістик Плюс
business_type: logistics
team_size: 18
report_type: cashflow

inflows:
  - article: Оплата від замовників — доставка
    responsible: Менеджер відділу продажів (Сергій)
    ops_per_month: 45
    has_sheets_access: true
```

This mode should:

1. Be deterministic.
2. Use `tzParser.js` first.
3. Avoid custom/free-text routing unless parsing genuinely failed.
4. Ask only follow-up clarification questions needed for build.
5. Never regress because of free-text experimentation.

### Mode 2: Free Text

Examples:

```text
У мене логістична компанія. Хочу кешфлоу. Менеджер веде оплату від клієнтів, водії витрачають пальне, бухгалтер зводить усе.
```

This mode can:

1. Use AI and heuristics.
2. Ask broader clarification questions.
3. Be more flexible.
4. Fail gracefully without breaking structured TZ behavior.

## Concrete Bug Seen Right Now

Structured TZ is parsed correctly.

Then the bot asks clarification questions like:

1. How is payment done for fuel?
2. If it is accountable, how should data be entered?
3. How is payment done for repairs?

Then the user replies with one short free-text answer:

```text
все через бухгалетера
```

Expected meaning:

1. All relevant payment-flow questions should be understood as centralized accountant payment.
2. Input-method questions like Google Form / separate sheet should become irrelevant.
3. The bot should not keep re-asking those irrelevant questions.

Actual behavior:

1. The bot applies the answer only partially.
2. It keeps asking already-covered or now-irrelevant questions.
3. It may repeat questions for the next person/article even though the user intended one global answer.

Example symptom:

User says:

```text
все через бухгалетера
```

Bot still asks:

1. fuel input method
2. repair payment mode
3. repair input method

This produces a loop-like clarification UX and blocks the actual table build.

## Why This Is Hard

We do not want to solve this with regexes like:

1. бухгалтер
2. бухгалетер
3. бух
4. через бух
5. оплачує бухгалтер

That approach does not scale because every user writes differently.

We want semantic understanding via AI, but we do not want AI changes to destabilize the structured-TZ build flow.

## Core Architectural Question

How should the Telegram state machine be designed so that:

1. Structured TZ is deterministic and protected.
2. Free-text mode remains AI-driven.
3. Clarification answers can still be semantically resolved by AI.
4. AI answer resolution can mark some questions as answered and other dependent questions as irrelevant.
5. The bot never re-asks already-covered clarifications in a loop.

## Suggested Conceptual Direction

The problem likely needs a cleaner separation of concerns:

1. Input classification layer:
   - structured_tz
   - free_text

2. Separate clarification engines:
   - deterministic clarification queue for structured TZ
   - AI-guided clarification for free-text

3. Question dependency graph:
   Example:
   - `money_flow_0`
   - `no_access_method_0` depends on `money_flow_0` being accountable, not centralized

4. AI answer resolution layer:
   Input:
   - current user answer
   - pending questions
   - full question queue
   - resolved answers so far
   - extracted TZ context

   Output:
   - resolved answer keys
   - skipped keys
   - confidence

5. Fallback logic:
   If AI is unavailable, use simple deterministic fallback, but do not let that fallback distort the structured flow.

## Important Constraint

If free-text mode cannot be made good enough immediately, that is acceptable.

What is not acceptable is breaking the structured TZ mode.

So the practical priority order is:

1. Structured TZ must work correctly.
2. Free-text should improve independently.

## What We Need From Another AI

We need a concrete design recommendation for the Telegram flow and clarification handling.

Please answer these:

1. How should the state machine be split between structured TZ and free-text?
2. How should dependent clarification questions be modeled so that irrelevant ones can be skipped safely?
3. How should AI be used for answer resolution without destabilizing deterministic structured-TZ flow?
4. How should the bot decide that a short answer like `все через бухгалетера` applies globally to multiple payment questions?
5. How should the bot decide which follow-up questions to remove from the queue after that?

## Secondary Issue

There is also an Apps Script validation issue after build in some scenarios, but that is not the main blocker in this handoff.

The main blocker right now is the Telegram clarification flow and preserving stable table building for structured TZ.