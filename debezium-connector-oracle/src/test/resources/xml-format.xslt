<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xslt="http://xml.apache.org/xslt" xmlns:xalan="xml.apache.org/xalan">
    <xsl:output indent="yes" omit-xml-declaration="yes" encoding="utf-8" xslt:indent-amount="2" xalan:indent-amount="2" />
    <xsl:strip-space elements="*"/>

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>