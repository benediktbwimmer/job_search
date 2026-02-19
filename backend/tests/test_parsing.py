import unittest
from datetime import datetime, timezone

from job_search.ingestion import (
    _extract_karriere_jobposting_from_html,
    _extract_indeed_jobposting_from_html,
    _extract_stepstone_detail_from_snapshot,
    _extract_stepstone_jobposting_from_html,
    parse_greenhouse_jobs,
    parse_indeed_listing_html,
    parse_karriere_html,
    parse_lever_jobs,
    parse_rss,
    parse_stepstone_listing_html,
    parse_stepstone_snapshot,
)


class ParsingTests(unittest.TestCase):
    def test_parse_rss_extracts_basic_fields(self):
        xml = """
        <rss><channel><item>
          <title>Senior Python Engineer at ExampleCo</title>
          <link>https://example.com/jobs/1</link>
          <description><![CDATA[<p>Fully remote in Europe</p>]]></description>
          <pubDate>Mon, 01 Jan 2026 10:00:00 +0000</pubDate>
          <guid>job-1</guid>
        </item></channel></rss>
        """
        out = parse_rss(xml, "Demo RSS", "remote")
        self.assertEqual(len(out), 1)
        job = out[0]
        self.assertEqual(job["id"], "Demo RSS:job-1")
        self.assertEqual(job["url"], "https://example.com/jobs/1")
        self.assertEqual(job["company"], "ExampleCo")
        self.assertTrue(job["remote_hint"])
        self.assertIn("Europe", job["location"])

    def test_parse_karriere_html_dedupes_ids(self):
        html = (
            '<a href="https://www.karriere.at/jobs/1234567">A</a>'
            '<a href="/jobs/1234567">A2</a>'
            '<a href="/jobs/7654321">B</a>'
        )
        out = parse_karriere_html(html, "Karriere", "innsbruck")
        self.assertEqual(len(out), 2)
        urls = sorted(x["url"] for x in out)
        self.assertEqual(
            urls,
            [
                "https://www.karriere.at/jobs/1234567",
                "https://www.karriere.at/jobs/7654321",
            ],
        )

    def test_parse_karriere_html_supports_modern_absolute_job_links(self):
        html = (
            '<a class="m-jobsListItem" href="https://www.karriere.at/jobs/7714505">Job 1</a>'
            '<a class="m-jobsListItem" href="https://www.karriere.at/f/acme">Company</a>'
            '<a class="m-jobsListItem" href="https://www.karriere.at/jobs/software-engineer/7745735?foo=bar">Job 2</a>'
            '<a href="/jobs/7745735">duplicate</a>'
        )
        out = parse_karriere_html(html, "Karriere", "innsbruck")
        urls = sorted(x["url"] for x in out)
        self.assertEqual(
            urls,
            [
                "https://www.karriere.at/jobs/7714505",
                "https://www.karriere.at/jobs/7745735",
            ],
        )

    def test_parse_greenhouse_jobs_extracts_fields(self):
        payload = """
        {
          "jobs": [
            {
              "id": 101,
              "title": "Senior Platform Engineer",
              "absolute_url": "https://boards.greenhouse.io/acme/jobs/101",
              "content": "<p>Remote in Europe. Salary: $140,000 - $170,000 per year</p>",
              "location": {"name": "Remote - Europe"},
              "updated_at": "2026-01-10T10:00:00Z"
            }
          ]
        }
        """
        out = parse_greenhouse_jobs(payload, "Acme Greenhouse", "remote", company_hint="Acme")
        self.assertEqual(len(out), 1)
        job = out[0]
        self.assertEqual(job["company"], "Acme")
        self.assertEqual(job["url"], "https://boards.greenhouse.io/acme/jobs/101")
        self.assertTrue(job["remote_hint"])
        self.assertIn("Europe", job["location"])

    def test_parse_lever_jobs_extracts_fields(self):
        payload = """
        [
          {
            "id": "abc",
            "text": "Software Engineer",
            "hostedUrl": "https://jobs.lever.co/acme/abc",
            "descriptionPlain": "Remote in Europe. Salary: €80,000 per year",
            "categories": {"location": "Europe", "team": "Platform"},
            "createdAt": 1760000000000
          }
        ]
        """
        out = parse_lever_jobs(payload, "Acme Lever", "remote", company_hint="Acme")
        self.assertEqual(len(out), 1)
        job = out[0]
        self.assertEqual(job["company"], "Acme")
        self.assertEqual(job["url"], "https://jobs.lever.co/acme/abc")
        self.assertTrue(job["remote_hint"])
        self.assertIn("Europe", job["location"])
        self.assertTrue(job["published"])

    def test_parse_indeed_listing_html_reads_mosaic_provider_data(self):
        html = """
        <html><head></head><body>
        <script>
        window.mosaic.providerData["mosaic-provider-jobcards"]={"metaData":{"mosaicProviderJobCardsModel":{"results":[
          {"jobkey":"abc123","displayTitle":"Senior Software Engineer","company":"Acme GmbH","formattedLocation":"Innsbruck","formattedRelativeTime":"vor 3 Tagen","pubDate":1770789600000,"snippet":"<ul><li>Build APIs</li></ul>"},
          {"jobkey":"abc123","displayTitle":"duplicate should be ignored","company":"Acme GmbH","formattedLocation":"Innsbruck","formattedRelativeTime":"vor 2 Tagen","snippet":"<p>Duplicate</p>"}
        ]}}};
        </script>
        </body></html>
        """
        out = parse_indeed_listing_html(
            html_text=html,
            source_name="Indeed Austria",
            source_type="austria",
            fetched_at="2026-02-18T02:00:00+00:00",
            source_url="https://at.indeed.com/jobs?q=software+engineer&l=Austria",
        )
        self.assertEqual(len(out), 1)
        item = out[0]
        self.assertEqual(item["title"], "Senior Software Engineer")
        self.assertEqual(item["company"], "Acme GmbH")
        self.assertEqual(item["location"], "Innsbruck")
        self.assertEqual(item["url"], "https://at.indeed.com/viewjob?jk=abc123")
        self.assertIn("Build APIs", item["description"])
        self.assertEqual(item["fetched_at"], "2026-02-18T02:00:00+00:00")
        dt = datetime.fromisoformat(item["published"])
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_extract_indeed_jobposting_from_html(self):
        html = """
        <html><head></head><body>
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Cloud Platform Engineer","datePosted":"2026-02-18T11:12:13+01:00","jobLocationType":"TELECOMMUTE","hiringOrganization":{"@type":"Organization","name":"Acme Labs"},"jobLocation":{"@type":"Place","address":{"@type":"PostalAddress","addressLocality":"Wien"}},"description":"<p>Build and run distributed systems</p>"}
        </script>
        </body></html>
        """
        parsed = _extract_indeed_jobposting_from_html(html)
        self.assertEqual(parsed["title"], "Cloud Platform Engineer")
        self.assertEqual(parsed["company"], "Acme Labs")
        self.assertEqual(parsed["location"], "Wien")
        self.assertTrue(parsed["remote_hint"])
        self.assertIn("distributed systems", parsed["description"])
        published_dt = datetime.fromisoformat(parsed["published"])
        self.assertEqual(published_dt.tzinfo, timezone.utc)
        self.assertEqual(published_dt.isoformat(), "2026-02-18T10:12:13+00:00")

    def test_parse_stepstone_snapshot_extracts_clean_cards(self):
        snapshot = """
        - heading "Python Developer (m/f/x)" [level=2] [ref=e649]:
          - link "Python Developer (m/f/x)" [ref=e650] [cursor=pointer]:
            - /url: /stellenangebote--Python-Developer-m-f-x-Innsbruck-Innerspace-GmbH--940142-inline.html
          - img "Innerspace GmbH" [ref=e648]
          - generic [ref=e663]: Innsbruck
          - generic [ref=e672]: "As a Python developer you build backend services and integrations for enterprise XR software. show more vor 1 Woche" [ref=e642]:
          - time [ref=e636]: vor 1 Woche
        - heading "Noch nichts dabei? Es gibt 21 weitere Jobs, die zu deiner Suche passen koennten" [level=2] [ref=e794]:
          - link "Noch nichts dabei" [ref=e795] [cursor=pointer]:
            - /url: /stellenangebote--Media-Sales-Specialist-m-w-d-Innsbruck-Iventa-The-Human-Management-Group--959410-inline.html
          - time [ref=e778]: vor 3 Tagen
        - heading "Senior Systems Engineer (m/w/d) Infrastructure & Kubernetes" [level=2] [ref=e745]:
          - link "Senior Systems Engineer (m/w/d) Infrastructure & Kubernetes" [ref=e746]:
            - /url: /stellenangebote--Senior-Systems-Engineer-m-w-d-Infrastructure-Kubernetes-Wattens-BERNARD-Gruppe-ZT-GmbH--961670-inline.html
          - img "BERNARD Gruppe ZT GmbH" [ref=e744]
          - generic [ref=e759]: Wattens
          - text: Die BERNARD Gruppe sucht einen Senior Systems Engineer fuer Infrastruktur und Kubernetes in Wattens.
          - time [ref=e738]: vor 6 Tagen
        """
        fetched_at = "2026-02-18T02:00:00+00:00"
        out = parse_stepstone_snapshot(
            snapshot=snapshot,
            source_name="StepStone Innsbruck Software Engineer",
            source_type="innsbruck",
            fetched_at=fetched_at,
        )
        self.assertEqual(len(out), 2)
        first = out[0]
        self.assertEqual(first["title"], "Python Developer (m/f/x)")
        self.assertEqual(first["company"], "Innerspace GmbH")
        self.assertIn("Innsbruck", first["location"])
        self.assertNotIn("[ref=", first["description"])
        self.assertNotIn("cursor=pointer", first["description"])
        self.assertEqual(first["fetched_at"], fetched_at)
        first_published = datetime.fromisoformat(first["published"])
        self.assertEqual(first_published.tzinfo, timezone.utc)
        self.assertTrue((datetime.fromisoformat(fetched_at) - first_published).days >= 6)

        second = out[1]
        self.assertEqual(second["title"], "Senior Systems Engineer (m/w/d) Infrastructure & Kubernetes")
        self.assertEqual(second["company"], "BERNARD Gruppe ZT GmbH")
        self.assertIn("Wattens", second["location"])
        self.assertTrue(second["published"])

    def test_extract_stepstone_detail_from_snapshot_extracts_rich_description(self):
        snapshot = """
        - heading "RezeptionistIn" [level=1] [ref=e115]
        - generic [ref=e143]: "Erschienen: vor 1 Tag"
        - article [ref=e173]:
          - paragraph [ref=e176]: Moechtest Du Teil eines Teams werden, das sich der hoechsten Qualitaet und dem exzellenten Service verschrieben hat?
          - paragraph [ref=e177]: Wir suchen begeisterte Mitarbeiter, die unsere Gaeste mit Professionalitaet und Leidenschaft betreuen.
        - article [ref=e185]:
          - heading "Aufgaben" [level=4] [ref=e189]
          - list [ref=e193]:
            - listitem [ref=e194]: Check in, Check out
            - listitem [ref=e195]: Betreuung der Gaeste waehrend des Aufenthaltes
        - contentinfo [ref=e783]
        """
        out = _extract_stepstone_detail_from_snapshot(snapshot)
        desc = str(out.get("description") or "")
        self.assertIn("Moechtest Du Teil eines Teams", desc)
        self.assertIn("- Check in, Check out", desc)
        self.assertIn("AUFGABEN", desc)
        self.assertTrue(out.get("published"))
        published_dt = datetime.fromisoformat(str(out["published"]))
        self.assertEqual(published_dt.tzinfo, timezone.utc)

    def test_parse_stepstone_listing_html_reads_preloaded_state(self):
        html = """
        <html><head></head><body>
        <script>
        window.__PRELOADED_STATE__["app-unifiedResultlist"] = {"searchResults":{"items":[
          {"id":961376,"title":"Python Developer (m/f/x)","companyName":"Innerspace GmbH","location":"Innsbruck","url":"/stellenangebote--Python-Developer-m-f-x-Innsbruck-Innerspace-GmbH--940142-inline.html?rltr=1","datePosted":"2026-02-06T23:01:12.41Z","textSnippet":"As a <strong>Python</strong> developer"}
        ]}};
        </script>
        </body></html>
        """
        out = parse_stepstone_listing_html(
            html_text=html,
            source_name="StepStone",
            source_type="innsbruck",
            fetched_at="2026-02-18T02:00:00+00:00",
        )
        self.assertEqual(len(out), 1)
        item = out[0]
        self.assertEqual(item["title"], "Python Developer (m/f/x)")
        self.assertEqual(item["company"], "Innerspace GmbH")
        self.assertEqual(item["location"], "Innsbruck")
        self.assertTrue(item["url"].startswith("https://www.stepstone.at/stellenangebote--Python-Developer"))
        self.assertIn("As a Python developer", item["description"])
        self.assertEqual(item["fetched_at"], "2026-02-18T02:00:00+00:00")
        dt = datetime.fromisoformat(item["published"])
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_extract_karriere_jobposting_from_html(self):
        html = """
        <html><head></head><body>
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Software Engineer (m/w/d)","datePosted":"2026-02-15T05:30:04+01:00","hiringOrganization":{"@type":"Organization","name":"Acme GmbH"},"jobLocation":{"@type":"Place","address":{"@type":"PostalAddress","addressLocality":"Innsbruck"}},"description":"<p>Build APIs and services</p>"}
        </script>
        </body></html>
        """
        parsed = _extract_karriere_jobposting_from_html(html)
        self.assertEqual(parsed["title"], "Software Engineer (m/w/d)")
        self.assertEqual(parsed["company"], "Acme GmbH")
        self.assertEqual(parsed["location"], "Innsbruck")
        self.assertIn("Build APIs", parsed["description"])
        published_dt = datetime.fromisoformat(parsed["published"])
        self.assertEqual(published_dt.tzinfo, timezone.utc)
        self.assertEqual(published_dt.isoformat(), "2026-02-15T04:30:04+00:00")

    def test_extract_stepstone_jobposting_from_html(self):
        html = """
        <html><head></head><body>
        <script type="application/ld+json">
          {"@context":"http://schema.org","@type":"JobPosting","title":"Senior Software Engineer","datePosted":"2026-02-13T10:00:00Z","hiringOrganization":{"@type":"Organization","name":"Acme GmbH"},"jobLocation":{"@type":"Place","address":{"@type":"PostalAddress","addressLocality":"Innsbruck"}},"description":"<h4>Tasks</h4><ul><li>Build APIs</li><li>Improve reliability</li></ul>"}
        </script>
        </body></html>
        """
        parsed = _extract_stepstone_jobposting_from_html(html)
        self.assertEqual(parsed["title"], "Senior Software Engineer")
        self.assertEqual(parsed["company"], "Acme GmbH")
        self.assertEqual(parsed["location"], "Innsbruck")
        self.assertIn("Tasks", parsed["description"])
        self.assertIn("- Build APIs", parsed["description"])
        published_dt = datetime.fromisoformat(parsed["published"])
        self.assertEqual(published_dt.tzinfo, timezone.utc)


if __name__ == "__main__":
    unittest.main()
