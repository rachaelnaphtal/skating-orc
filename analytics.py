import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, text
from scipy import stats
from scipy.stats import linregress
from models import (
    Judge, Competition, Segment, DisciplineType, ElementType, 
    PcsScorePerJudge, ElementScorePerJudge, Element, SkaterSegment,
    Skater, PcsType
)

class JudgeAnalytics:
    def __init__(self, session: Session):
        self.session = session
    
    def get_judges(self):
        """Get all judges in alphabetical order"""
        judges = self.session.query(Judge).order_by(Judge.name).all()
        return [(judge.id, judge.name, judge.location) for judge in judges]
    
    def get_competitions(self):
        """Get all competitions"""
        competitions = self.session.query(Competition).order_by(Competition.year.desc(), Competition.name).all()
        return [(comp.id, comp.name, comp.year) for comp in competitions]
        
    def get_judge_competitions(self, judge_id):
        """Get competitions where a specific judge participated"""
        # Get competitions from PCS scores
        pcs_competitions = self.session.query(Competition).join(
            Segment, Segment.competition_id == Competition.id
        ).join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            PcsScorePerJudge, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).filter(
            PcsScorePerJudge.judge_id == judge_id
        ).distinct()
        
        # Get competitions from element scores  
        element_competitions = self.session.query(Competition).join(
            Segment, Segment.competition_id == Competition.id
        ).join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            Element, Element.skater_segment_id == SkaterSegment.id
        ).join(
            ElementScorePerJudge, ElementScorePerJudge.element_id == Element.id
        ).filter(
            ElementScorePerJudge.judge_id == judge_id
        ).distinct()
        
        # Combine and deduplicate
        all_competitions = set()
        for comp in pcs_competitions:
            all_competitions.add((comp.id, comp.name, comp.year))
        for comp in element_competitions:
            all_competitions.add((comp.id, comp.name, comp.year))
            
        # Sort by year desc, then name
        return sorted(list(all_competitions), key=lambda x: (x[2], x[1]), reverse=True)
    
    def get_judge_segment_stats(self, judge_id, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get segment statistics for a specific judge"""
        # Base query for segments this judge scored
        segment_query = self.session.query(Segment).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        )
        
        # Apply filters
        if year_filter:
            segment_query = segment_query.filter(Competition.year == year_filter)
        if competition_ids:
            segment_query = segment_query.filter(Segment.competition_id.in_(competition_ids))
        if discipline_type_ids:
            segment_query = segment_query.filter(Segment.discipline_type_id.in_(discipline_type_ids))
        
        # Get segments where this judge has PCS scores
        pcs_segments = segment_query.join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            PcsScorePerJudge, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).filter(
            PcsScorePerJudge.judge_id == judge_id
        ).distinct()
        
        # Get segments where this judge has element scores
        element_segments = segment_query.join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            Element, Element.skater_segment_id == SkaterSegment.id
        ).join(
            ElementScorePerJudge, ElementScorePerJudge.element_id == Element.id
        ).filter(
            ElementScorePerJudge.judge_id == judge_id
        ).distinct()
        
        segment_stats = []
        all_segment_ids = set()
        
        # Collect all segments
        for segment in pcs_segments:
            all_segment_ids.add(segment.id)
        for segment in element_segments:
            all_segment_ids.add(segment.id)
        
        for segment_id in all_segment_ids:
            segment = self.session.query(Segment).filter(Segment.id == segment_id).first()
            if not segment:
                continue
                
            # Count skaters in this segment
            skater_count = self.session.query(SkaterSegment).filter(
                SkaterSegment.segment_id == segment_id
            ).count()
            
            # Count PCS anomalies
            pcs_anomalies = self.session.query(PcsScorePerJudge).join(
                SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
            ).filter(
                PcsScorePerJudge.judge_id == judge_id,
                SkaterSegment.segment_id == segment_id,
                func.abs(PcsScorePerJudge.deviation) >= 1.5
            ).count()
            
            # Count element anomalies
            element_anomalies = self.session.query(ElementScorePerJudge).join(
                Element, ElementScorePerJudge.element_id == Element.id
            ).join(
                SkaterSegment, Element.skater_segment_id == SkaterSegment.id
            ).filter(
                ElementScorePerJudge.judge_id == judge_id,
                SkaterSegment.segment_id == segment_id,
                func.abs(ElementScorePerJudge.deviation) >= 2.0
            ).count()
            
            # Count rule errors  
            pcs_rule_errors = self.session.query(PcsScorePerJudge).join(
                SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
            ).filter(
                PcsScorePerJudge.judge_id == judge_id,
                SkaterSegment.segment_id == segment_id,
                PcsScorePerJudge.is_rule_error == True
            ).count()
            
            element_rule_errors = self.session.query(ElementScorePerJudge).join(
                Element, ElementScorePerJudge.element_id == Element.id
            ).join(
                SkaterSegment, Element.skater_segment_id == SkaterSegment.id
            ).filter(
                ElementScorePerJudge.judge_id == judge_id,
                SkaterSegment.segment_id == segment_id,
                ElementScorePerJudge.is_rule_error == True
            ).count()
            
            total_anomalies = pcs_anomalies + element_anomalies
            total_rule_errors = pcs_rule_errors + element_rule_errors
            
            segment_stats.append({
                'segment_id': segment_id,
                'competition_name': segment.competition.name,
                'competition_year': segment.competition.year,
                'discipline': segment.discipline_type.name,
                'segment_name': segment.name,
                'skater_count': skater_count,
                'total_anomalies': total_anomalies,
                'pcs_anomalies': pcs_anomalies,
                'element_anomalies': element_anomalies,
                'total_rule_errors': total_rule_errors,
                'pcs_rule_errors': pcs_rule_errors,
                'element_rule_errors': element_rule_errors
            })
        
        return pd.DataFrame(segment_stats)
    
    def get_competition_segment_statistics(self, competition_id):
        """Get segment statistics for all judges in a specific competition"""
        # Get all segments in this competition
        segments = self.session.query(Segment).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        ).filter(
            Competition.id == competition_id
        ).all()
        
        # Get all judges who participated in this competition (from both PCS and element scores)
        pcs_judges = self.session.query(Judge.id, Judge.name).join(
            PcsScorePerJudge, Judge.id == PcsScorePerJudge.judge_id
        ).join(
            SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).join(
            Segment, SkaterSegment.segment_id == Segment.id
        ).filter(
            Segment.competition_id == competition_id
        ).distinct().all()
        
        element_judges = self.session.query(Judge.id, Judge.name).join(
            ElementScorePerJudge, Judge.id == ElementScorePerJudge.judge_id
        ).join(
            Element, ElementScorePerJudge.element_id == Element.id
        ).join(
            SkaterSegment, Element.skater_segment_id == SkaterSegment.id
        ).join(
            Segment, SkaterSegment.segment_id == Segment.id
        ).filter(
            Segment.competition_id == competition_id
        ).distinct().all()
        
        # Combine judges
        all_judges = set(pcs_judges + element_judges)
        
        # Pre-calculate skater counts for all segments to avoid repeated queries
        segment_skater_counts = {}
        for segment in segments:
            segment_skater_counts[segment.id] = self.session.query(SkaterSegment).filter(
                SkaterSegment.segment_id == segment.id
            ).count()
        
        # Calculate statistics for each judge-segment combination
        segment_stats = []
        for segment in segments:
            for judge_id, judge_name in all_judges:
                # More efficient existence check using exists()
                has_pcs_scores = self.session.query(PcsScorePerJudge.id).join(
                    SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
                ).filter(
                    PcsScorePerJudge.judge_id == judge_id,
                    SkaterSegment.segment_id == segment.id
                ).first() is not None
                
                has_element_scores = self.session.query(ElementScorePerJudge.id).join(
                    Element, ElementScorePerJudge.element_id == Element.id
                ).join(
                    SkaterSegment, Element.skater_segment_id == SkaterSegment.id
                ).filter(
                    ElementScorePerJudge.judge_id == judge_id,
                    SkaterSegment.segment_id == segment.id
                ).first() is not None
                
                # Only process if judge actually scored this segment
                if not (has_pcs_scores or has_element_scores):
                    continue
                
                # Use pre-calculated skater count
                skater_count = segment_skater_counts[segment.id]
                
                # Count PCS anomalies
                pcs_anomalies = self.session.query(PcsScorePerJudge).join(
                    SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
                ).filter(
                    PcsScorePerJudge.judge_id == judge_id,
                    SkaterSegment.segment_id == segment.id,
                    func.abs(PcsScorePerJudge.deviation) >= 1.5
                ).count()
                
                # Count element anomalies
                element_anomalies = self.session.query(ElementScorePerJudge).join(
                    Element, ElementScorePerJudge.element_id == Element.id
                ).join(
                    SkaterSegment, Element.skater_segment_id == SkaterSegment.id
                ).filter(
                    ElementScorePerJudge.judge_id == judge_id,
                    SkaterSegment.segment_id == segment.id,
                    func.abs(ElementScorePerJudge.deviation) >= 2.0
                ).count()
                
                # Count rule errors
                pcs_rule_errors = self.session.query(PcsScorePerJudge).join(
                    SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
                ).filter(
                    PcsScorePerJudge.judge_id == judge_id,
                    SkaterSegment.segment_id == segment.id,
                    PcsScorePerJudge.is_rule_error == True
                ).count()
                
                element_rule_errors = self.session.query(ElementScorePerJudge).join(
                    Element, ElementScorePerJudge.element_id == Element.id
                ).join(
                    SkaterSegment, Element.skater_segment_id == SkaterSegment.id
                ).filter(
                    ElementScorePerJudge.judge_id == judge_id,
                    SkaterSegment.segment_id == segment.id,
                    ElementScorePerJudge.is_rule_error == True
                ).count()
                
                total_anomalies = pcs_anomalies + element_anomalies
                total_rule_errors = pcs_rule_errors + element_rule_errors
                
                segment_stats.append({
                    'judge_id': judge_id,
                    'judge_name': judge_name,
                    'segment_id': segment.id,
                    'segment_name': segment.name,
                    'competition_name': segment.competition.name,
                    'competition_year': segment.competition.year,
                    'discipline': segment.discipline_type.name,
                    'skater_count': skater_count,
                    'total_anomalies': total_anomalies,
                    'pcs_anomalies': pcs_anomalies,
                    'element_anomalies': element_anomalies,
                    'total_rule_errors': total_rule_errors,
                    'pcs_rule_errors': pcs_rule_errors,
                    'element_rule_errors': element_rule_errors
                })
        
        return pd.DataFrame(segment_stats)
    
    def get_all_rule_errors(self, year_filter=None, competition_ids=None, judge_ids=None):
        """Get all rule errors with optional filters"""
        # PCS rule errors
        pcs_query = self.session.query(
            PcsScorePerJudge.judge_id,
            Judge.name.label('judge_name'),
            Competition.name.label('competition_name'),
            Competition.year.label('competition_year'),
            Competition.results_url.label('competition_url'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name'),
            PcsType.name.label('score_type'),
            PcsScorePerJudge.judge_score,
            PcsScorePerJudge.panel_average,
            PcsScorePerJudge.deviation,

        ).join(
            Judge, PcsScorePerJudge.judge_id == Judge.id
        ).join(
            SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).join(
            Segment, SkaterSegment.segment_id == Segment.id
        ).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        ).join(
            Skater, SkaterSegment.skater_id == Skater.id
        ).join(
            PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id
        ).filter(
            PcsScorePerJudge.is_rule_error == True
        )
        
        # Element rule errors
        element_query = self.session.query(
            ElementScorePerJudge.judge_id,
            Judge.name.label('judge_name'),
            Competition.name.label('competition_name'),
            Competition.year.label('competition_year'),
            Competition.results_url.label('competition_url'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name'),
            Element.name.label('element_name'),
            ElementType.name.label('element_type'),
            ElementScorePerJudge.judge_score,
            ElementScorePerJudge.panel_average,
            ElementScorePerJudge.deviation
        ).join(
            Judge, ElementScorePerJudge.judge_id == Judge.id
        ).join(
            Element, ElementScorePerJudge.element_id == Element.id
        ).join(
            SkaterSegment, Element.skater_segment_id == SkaterSegment.id
        ).join(
            Segment, SkaterSegment.segment_id == Segment.id
        ).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        ).join(
            Skater, SkaterSegment.skater_id == Skater.id
        ).join(
            ElementType, Element.element_type_id == ElementType.id
        ).filter(
            ElementScorePerJudge.is_rule_error == True
        )
        
        # Apply filters to both queries
        if year_filter:
            pcs_query = pcs_query.filter(Competition.year == year_filter)
            element_query = element_query.filter(Competition.year == year_filter)
        if competition_ids:
            pcs_query = pcs_query.filter(Competition.id.in_(competition_ids))
            element_query = element_query.filter(Competition.id.in_(competition_ids))
        if judge_ids:
            pcs_query = pcs_query.filter(Judge.id.in_(judge_ids))
            element_query = element_query.filter(Judge.id.in_(judge_ids))
        
        # Execute queries and convert to DataFrames
        pcs_results = pcs_query.all()
        element_results = element_query.all()
        
        # Convert to DataFrames and add category
        pcs_data = []
        for result in pcs_results:
            pcs_data.append({
                'judge_id': result.judge_id,
                'judge_name': result.judge_name,
                'competition_name': result.competition_name,
                'competition_year': result.competition_year,
                'competition_url': result.competition_url,
                'segment_name': result.segment_name,
                'discipline_name': result.discipline_name,
                'skater_name': result.skater_name,
                'element_name': '',  # PCS doesn't have element name
                'element_type': result.score_type,
                'judge_score': result.judge_score,
                'panel_average': result.panel_average,
                'deviation': result.deviation
            })
            
        element_data = []
        for result in element_results:
            element_data.append({
                'judge_id': result.judge_id,
                'judge_name': result.judge_name,
                'competition_name': result.competition_name,
                'competition_year': result.competition_year,
                'competition_url': result.competition_url,
                'segment_name': result.segment_name,
                'discipline_name': result.discipline_name,
                'skater_name': result.skater_name,
                'element_name': result.element_name,
                'element_type': result.element_type,
                'judge_score': result.judge_score,
                'panel_average': result.panel_average,
                'deviation': result.deviation
            })
        
        # Combine results
        all_data = pcs_data + element_data
        return pd.DataFrame(all_data)
    
    def get_years(self):
        """Get all unique years"""
        years = self.session.query(Competition.year).distinct().order_by(Competition.year.desc()).all()
        return [year[0] for year in years]
    
    def get_discipline_types(self):
        """Get all discipline types"""
        discipline_types = self.session.query(DisciplineType).all()
        return [(dt.id, dt.name) for dt in discipline_types]
    
    def get_element_types(self):
        """Get all element types"""
        element_types = self.session.query(ElementType).all()
        return [(et.id, et.name) for et in element_types]
    
    def get_judge_pcs_stats(self, judge_id, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get PCS statistics for a specific judge"""
        query = self.session.query(
            PcsScorePerJudge.thrown_out,
            PcsScorePerJudge.deviation,
            PcsScorePerJudge.judge_score,
            PcsScorePerJudge.panel_average,
            PcsScorePerJudge.is_rule_error,
            PcsType.name.label('pcs_type_name'),
            Competition.name.label('competition_name'),
            Competition.results_url.label('competition_url'),
            Competition.year,
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name')
        ).join(Judge, PcsScorePerJudge.judge_id == Judge.id)\
         .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)\
         .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .join(Skater, SkaterSegment.skater_id == Skater.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id == judge_id)
        
        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if discipline_type_ids:
            query = query.filter(Segment.discipline_type_id.in_(discipline_type_ids))
        
        results = query.all()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'judge_score': float(r.judge_score),
            'panel_average': float(r.panel_average),
            'is_rule_error': r.is_rule_error,
            'pcs_type_name': r.pcs_type_name,
            'competition_name': r.competition_name,
            'competition_url': r.competition_url,
            'year': r.year,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'skater_name': r.skater_name,
            'anomaly': abs(float(r.deviation)) >= 1.5
        } for r in results])
        
        return df
    
    def get_judge_element_stats(self, judge_id, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get element statistics for a specific judge"""
        query = self.session.query(
            ElementScorePerJudge.thrown_out,
            ElementScorePerJudge.deviation,
            ElementScorePerJudge.judge_score,
            ElementScorePerJudge.panel_average,
            ElementScorePerJudge.is_rule_error,
            Element.name.label('element_name'),
            Element.element_type,
            ElementType.name.label('element_type_name'),
            Competition.name.label('competition_name'),
            Competition.results_url.label('competition_url'),
            Competition.year,
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name')
        ).join(Judge, ElementScorePerJudge.judge_id == Judge.id)\
         .join(Element, ElementScorePerJudge.element_id == Element.id)\
         .outerjoin(ElementType, Element.element_type_id == ElementType.id)\
         .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .join(Skater, SkaterSegment.skater_id == Skater.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id == judge_id)
        
        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if discipline_type_ids:
            query = query.filter(Segment.discipline_type_id.in_(discipline_type_ids))
        
        results = query.all()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'judge_score': float(r.judge_score),
            'panel_average': float(r.panel_average),
            'is_rule_error': r.is_rule_error,
            'element_name': r.element_name,
            'element_type': r.element_type,
            'element_type_name': r.element_type_name or r.element_type,
            'competition_name': r.competition_name,
            'competition_url': r.competition_url,
            'year': r.year,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'skater_name': r.skater_name,
            'anomaly': abs(float(r.deviation)) >= 2.0
        } for r in results])
        
        return df
    
    def get_multi_judge_pcs_comparison(self, judge_ids, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get PCS comparison data for multiple judges"""
        query = self.session.query(
            Judge.id.label('judge_id'),
            Judge.name.label('judge_name'),
            PcsScorePerJudge.thrown_out,
            PcsScorePerJudge.deviation,
            PcsScorePerJudge.is_rule_error,
            PcsType.name.label('pcs_type_name'),
            Competition.year,
            Competition.name.label('competition_name'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name')
        ).join(PcsScorePerJudge, Judge.id == PcsScorePerJudge.judge_id)\
         .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)\
         .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id.in_(judge_ids))
        
        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if discipline_type_ids:
            query = query.filter(Segment.discipline_type_id.in_(discipline_type_ids))
        
        results = query.all()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'judge_id': r.judge_id,
            'judge_name': r.judge_name,
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'is_rule_error': r.is_rule_error,
            'pcs_type_name': r.pcs_type_name,
            'year': r.year,
            'competition_name': r.competition_name,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'anomaly': abs(float(r.deviation)) >= 1.5
        } for r in results])
        
        return df
    
    def get_multi_judge_element_comparison(self, judge_ids, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get element comparison data for multiple judges"""
        query = self.session.query(
            Judge.id.label('judge_id'),
            Judge.name.label('judge_name'),
            ElementScorePerJudge.thrown_out,
            ElementScorePerJudge.deviation,
            ElementScorePerJudge.is_rule_error,
            Element.element_type,
            ElementType.name.label('element_type_name'),
            Competition.year,
            Competition.name.label('competition_name'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name')
        ).join(ElementScorePerJudge, Judge.id == ElementScorePerJudge.judge_id)\
         .join(Element, ElementScorePerJudge.element_id == Element.id)\
         .outerjoin(ElementType, Element.element_type_id == ElementType.id)\
         .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id.in_(judge_ids))
        
        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if discipline_type_ids:
            query = query.filter(Segment.discipline_type_id.in_(discipline_type_ids))
        
        results = query.all()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'judge_id': r.judge_id,
            'judge_name': r.judge_name,
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'is_rule_error': r.is_rule_error,
            'element_type': r.element_type,
            'element_type_name': r.element_type_name or r.element_type,
            'year': r.year,
            'competition_name': r.competition_name,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'anomaly': abs(float(r.deviation)) >= 2.0
        } for r in results])
        
        return df
    
    def calculate_judge_summary_stats(self, pcs_df, element_df):
        """Calculate summary statistics for a judge"""
        stats = {}
        
        # PCS Statistics
        if not pcs_df.empty:
            stats['pcs_total_scores'] = len(pcs_df)
            stats['pcs_throwout_rate'] = (pcs_df['thrown_out'].sum() / len(pcs_df)) * 100
            stats['pcs_anomaly_rate'] = (pcs_df['anomaly'].sum() / len(pcs_df)) * 100
            stats['pcs_rule_error_rate'] = (pcs_df['is_rule_error'].sum() / len(pcs_df)) * 100
            stats['pcs_avg_deviation'] = pcs_df['deviation'].mean()
        else:
            stats['pcs_total_scores'] = 0
            stats['pcs_throwout_rate'] = 0
            stats['pcs_anomaly_rate'] = 0
            stats['pcs_rule_error_rate'] = 0
            stats['pcs_avg_deviation'] = 0
        
        # Element Statistics
        if not element_df.empty:
            stats['element_total_scores'] = len(element_df)
            stats['element_throwout_rate'] = (element_df['thrown_out'].sum() / len(element_df)) * 100
            stats['element_anomaly_rate'] = (element_df['anomaly'].sum() / len(element_df)) * 100
            stats['element_rule_error_rate'] = (element_df['is_rule_error'].sum() / len(element_df)) * 100
            stats['element_avg_deviation'] = element_df['deviation'].mean()
        else:
            stats['element_total_scores'] = 0
            stats['element_throwout_rate'] = 0
            stats['element_anomaly_rate'] = 0
            stats['element_rule_error_rate'] = 0
            stats['element_avg_deviation'] = 0
        
        return stats
    
    def get_judge_performance_heatmap_data(self, metric='throwout_rate', score_type='both', year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get data for judge performance heatmap"""
        
        # Base query for judges
        judge_query = self.session.query(Judge.id, Judge.name).filter(Judge.id.in_(
            self.session.query(PcsScorePerJudge.judge_id).union(
                self.session.query(ElementScorePerJudge.judge_id)
            )
        ))
        
        judges = judge_query.all()
        judge_data = []
        
        for judge_id, judge_name in judges:
            # Get PCS and Element data for this judge
            pcs_df = self.get_judge_pcs_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
            element_df = self.get_judge_element_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
            
            if pcs_df.empty and element_df.empty:
                continue
                
            stats = self.calculate_judge_summary_stats(pcs_df, element_df)
            
            # Calculate the requested metric
            if metric == 'throwout_rate':
                if score_type == 'pcs':
                    value = stats['pcs_throwout_rate']
                elif score_type == 'element':
                    value = stats['element_throwout_rate']
                else:  # both
                    total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                    if total_scores > 0:
                        total_throwouts = (stats['pcs_throwout_rate'] * stats['pcs_total_scores'] / 100) + \
                                        (stats['element_throwout_rate'] * stats['element_total_scores'] / 100)
                        value = (total_throwouts / total_scores) * 100
                    else:
                        value = 0
            elif metric == 'anomaly_rate':
                if score_type == 'pcs':
                    value = stats['pcs_anomaly_rate']
                elif score_type == 'element':
                    value = stats['element_anomaly_rate']
                else:  # both
                    total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                    if total_scores > 0:
                        total_anomalies = (stats['pcs_anomaly_rate'] * stats['pcs_total_scores'] / 100) + \
                                        (stats['element_anomaly_rate'] * stats['element_total_scores'] / 100)
                        value = (total_anomalies / total_scores) * 100
                    else:
                        value = 0
            elif metric == 'rule_error_rate':
                if score_type == 'pcs':
                    value = stats['pcs_rule_error_rate']
                elif score_type == 'element':
                    value = stats['element_rule_error_rate']
                else:  # both
                    total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                    if total_scores > 0:
                        total_rule_errors = (stats['pcs_rule_error_rate'] * stats['pcs_total_scores'] / 100) + \
                                          (stats['element_rule_error_rate'] * stats['element_total_scores'] / 100)
                        value = (total_rule_errors / total_scores) * 100
                    else:
                        value = 0
            else:  # avg_deviation
                if score_type == 'pcs':
                    value = abs(stats['pcs_avg_deviation']) if stats['pcs_total_scores'] > 0 else 0
                elif score_type == 'element':
                    value = abs(stats['element_avg_deviation']) if stats['element_total_scores'] > 0 else 0
                else:  # both
                    total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                    if total_scores > 0:
                        weighted_avg = (abs(stats['pcs_avg_deviation']) * stats['pcs_total_scores'] + \
                                      abs(stats['element_avg_deviation']) * stats['element_total_scores']) / total_scores
                        value = weighted_avg
                    else:
                        value = 0
            
            judge_data.append({
                'judge_id': judge_id,
                'judge_name': judge_name,
                'metric_value': round(value, 2),
                'total_scores': stats['pcs_total_scores'] + stats['element_total_scores'],
                'pcs_scores': stats['pcs_total_scores'],
                'element_scores': stats['element_total_scores']
            })
        
        return pd.DataFrame(judge_data)
    
    def get_judge_competition_heatmap_data(self, metric='throwout_rate', score_type='both'):
        """Get data for judge vs competition heatmap"""
        
        # Get all judges and competitions
        judges = self.session.query(Judge.id, Judge.name).all()
        competitions = self.session.query(Competition.id, Competition.name, Competition.year).all()
        
        heatmap_data = []
        
        for judge_id, judge_name in judges:
            for comp_id, comp_name, comp_year in competitions:
                # Get data for this judge and competition
                pcs_df = self.get_judge_pcs_stats(judge_id, None, [comp_id], None)
                element_df = self.get_judge_element_stats(judge_id, None, [comp_id], None)
                
                if pcs_df.empty and element_df.empty:
                    continue
                    
                stats = self.calculate_judge_summary_stats(pcs_df, element_df)
                
                # Calculate the requested metric
                if metric == 'throwout_rate':
                    if score_type == 'pcs':
                        value = stats['pcs_throwout_rate']
                    elif score_type == 'element':
                        value = stats['element_throwout_rate']
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            total_throwouts = (stats['pcs_throwout_rate'] * stats['pcs_total_scores'] / 100) + \
                                            (stats['element_throwout_rate'] * stats['element_total_scores'] / 100)
                            value = (total_throwouts / total_scores) * 100
                        else:
                            continue
                elif metric == 'anomaly_rate':
                    if score_type == 'pcs':
                        value = stats['pcs_anomaly_rate']
                    elif score_type == 'element':
                        value = stats['element_anomaly_rate']
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            total_anomalies = (stats['pcs_anomaly_rate'] * stats['pcs_total_scores'] / 100) + \
                                            (stats['element_anomaly_rate'] * stats['element_total_scores'] / 100)
                            value = (total_anomalies / total_scores) * 100
                        else:
                            continue
                elif metric == 'rule_error_rate':
                    if score_type == 'pcs':
                        value = stats['pcs_rule_error_rate']
                    elif score_type == 'element':
                        value = stats['element_rule_error_rate']
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            total_rule_errors = (stats['pcs_rule_error_rate'] * stats['pcs_total_scores'] / 100) + \
                                              (stats['element_rule_error_rate'] * stats['element_total_scores'] / 100)
                            value = (total_rule_errors / total_scores) * 100
                        else:
                            continue
                else:  # avg_deviation
                    if score_type == 'pcs':
                        value = abs(stats['pcs_avg_deviation']) if stats['pcs_total_scores'] > 0 else 0
                    elif score_type == 'element':
                        value = abs(stats['element_avg_deviation']) if stats['element_total_scores'] > 0 else 0
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            weighted_avg = (abs(stats['pcs_avg_deviation']) * stats['pcs_total_scores'] + \
                                          abs(stats['element_avg_deviation']) * stats['element_total_scores']) / total_scores
                            value = weighted_avg
                        else:
                            continue
                
                heatmap_data.append({
                    'judge_name': judge_name,
                    'competition': f"{comp_name} ({comp_year})",
                    'metric_value': round(value, 2),
                    'total_scores': stats['pcs_total_scores'] + stats['element_total_scores']
                })
        
        return pd.DataFrame(heatmap_data)
    
    def get_temporal_trends_data(self, judge_id=None, period='year', metric='throwout_rate', score_type='both'):
        """Get temporal trends data for judge consistency over time"""
        
        if period == 'year':
            time_field = 'year'
            time_label = 'Year'
        elif period == 'quarter':
            # Create quarters from competition dates or use year as fallback
            time_field = 'year'  # For now, using year as fallback
            time_label = 'Year'
        else:  # month
            time_field = 'year'  # For now, using year as fallback
            time_label = 'Year'
        
        trends_data = []
        
        if judge_id:
            # Single judge trends over time
            years = self.session.query(Competition.year).distinct().order_by(Competition.year).all()
            
            for year_tuple in years:
                year = year_tuple[0]
                
                # Get data for this judge and year
                pcs_df = self.get_judge_pcs_stats(judge_id, year, None, None)
                element_df = self.get_judge_element_stats(judge_id, year, None, None)
                
                if pcs_df.empty and element_df.empty:
                    continue
                    
                stats = self.calculate_judge_summary_stats(pcs_df, element_df)
                
                # Calculate the requested metric
                if metric == 'throwout_rate':
                    if score_type == 'pcs':
                        value = stats['pcs_throwout_rate']
                    elif score_type == 'element':
                        value = stats['element_throwout_rate']
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            total_throwouts = (stats['pcs_throwout_rate'] * stats['pcs_total_scores'] / 100) + \
                                            (stats['element_throwout_rate'] * stats['element_total_scores'] / 100)
                            value = (total_throwouts / total_scores) * 100
                        else:
                            continue
                elif metric == 'anomaly_rate':
                    if score_type == 'pcs':
                        value = stats['pcs_anomaly_rate']
                    elif score_type == 'element':
                        value = stats['element_anomaly_rate']
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            total_anomalies = (stats['pcs_anomaly_rate'] * stats['pcs_total_scores'] / 100) + \
                                            (stats['element_anomaly_rate'] * stats['element_total_scores'] / 100)
                            value = (total_anomalies / total_scores) * 100
                        else:
                            continue
                elif metric == 'rule_error_rate':
                    if score_type == 'pcs':
                        value = stats['pcs_rule_error_rate']
                    elif score_type == 'element':
                        value = stats['element_rule_error_rate']
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            total_rule_errors = (stats['pcs_rule_error_rate'] * stats['pcs_total_scores'] / 100) + \
                                              (stats['element_rule_error_rate'] * stats['element_total_scores'] / 100)
                            value = (total_rule_errors / total_scores) * 100
                        else:
                            continue
                else:  # avg_deviation
                    if score_type == 'pcs':
                        value = abs(stats['pcs_avg_deviation']) if stats['pcs_total_scores'] > 0 else 0
                    elif score_type == 'element':
                        value = abs(stats['element_avg_deviation']) if stats['element_total_scores'] > 0 else 0
                    else:  # both
                        total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                        if total_scores > 0:
                            weighted_avg = (abs(stats['pcs_avg_deviation']) * stats['pcs_total_scores'] + \
                                          abs(stats['element_avg_deviation']) * stats['element_total_scores']) / total_scores
                            value = weighted_avg
                        else:
                            continue
                
                # Get judge name
                judge_name = self.session.query(Judge.name).filter(Judge.id == judge_id).scalar()
                
                trends_data.append({
                    'judge_id': judge_id,
                    'judge_name': judge_name,
                    'time_period': year,
                    'metric_value': round(value, 2),
                    'total_scores': stats['pcs_total_scores'] + stats['element_total_scores'],
                    'pcs_scores': stats['pcs_total_scores'],
                    'element_scores': stats['element_total_scores']
                })
        else:
            # All judges trends over time (aggregated)
            judges = self.session.query(Judge.id, Judge.name).all()
            years = self.session.query(Competition.year).distinct().order_by(Competition.year).all()
            
            for year_tuple in years:
                year = year_tuple[0]
                year_metrics = []
                
                for judge_id, judge_name in judges:
                    # Get data for this judge and year
                    pcs_df = self.get_judge_pcs_stats(judge_id, year, None, None)
                    element_df = self.get_judge_element_stats(judge_id, year, None, None)
                    
                    if pcs_df.empty and element_df.empty:
                        continue
                        
                    stats = self.calculate_judge_summary_stats(pcs_df, element_df)
                    
                    # Calculate the requested metric
                    if metric == 'throwout_rate':
                        if score_type == 'pcs':
                            value = stats['pcs_throwout_rate']
                        elif score_type == 'element':
                            value = stats['element_throwout_rate']
                        else:  # both
                            total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                            if total_scores > 0:
                                total_throwouts = (stats['pcs_throwout_rate'] * stats['pcs_total_scores'] / 100) + \
                                                (stats['element_throwout_rate'] * stats['element_total_scores'] / 100)
                                value = (total_throwouts / total_scores) * 100
                            else:
                                continue
                    elif metric == 'anomaly_rate':
                        if score_type == 'pcs':
                            value = stats['pcs_anomaly_rate']
                        elif score_type == 'element':
                            value = stats['element_anomaly_rate']
                        else:  # both
                            total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                            if total_scores > 0:
                                total_anomalies = (stats['pcs_anomaly_rate'] * stats['pcs_total_scores'] / 100) + \
                                                (stats['element_anomaly_rate'] * stats['element_total_scores'] / 100)
                                value = (total_anomalies / total_scores) * 100
                            else:
                                continue
                    elif metric == 'rule_error_rate':
                        if score_type == 'pcs':
                            value = stats['pcs_rule_error_rate']
                        elif score_type == 'element':
                            value = stats['element_rule_error_rate']
                        else:  # both
                            total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                            if total_scores > 0:
                                total_rule_errors = (stats['pcs_rule_error_rate'] * stats['pcs_total_scores'] / 100) + \
                                                  (stats['element_rule_error_rate'] * stats['element_total_scores'] / 100)
                                value = (total_rule_errors / total_scores) * 100
                            else:
                                continue
                    else:  # avg_deviation
                        if score_type == 'pcs':
                            value = abs(stats['pcs_avg_deviation']) if stats['pcs_total_scores'] > 0 else 0
                        elif score_type == 'element':
                            value = abs(stats['element_avg_deviation']) if stats['element_total_scores'] > 0 else 0
                        else:  # both
                            total_scores = stats['pcs_total_scores'] + stats['element_total_scores']
                            if total_scores > 0:
                                weighted_avg = (abs(stats['pcs_avg_deviation']) * stats['pcs_total_scores'] + \
                                              abs(stats['element_avg_deviation']) * stats['element_total_scores']) / total_scores
                                value = weighted_avg
                            else:
                                continue
                    
                    year_metrics.append({
                        'judge_id': judge_id,
                        'judge_name': judge_name,
                        'value': value,
                        'total_scores': stats['pcs_total_scores'] + stats['element_total_scores']
                    })
                
                if year_metrics:
                    # Calculate aggregated metrics for this year
                    values = [m['value'] for m in year_metrics]
                    total_judges = len(year_metrics)
                    avg_value = np.mean(values)
                    median_value = np.median(values)
                    std_value = np.std(values)
                    
                    trends_data.append({
                        'time_period': year,
                        'avg_metric_value': round(avg_value, 2),
                        'median_metric_value': round(median_value, 2),
                        'std_metric_value': round(std_value, 2),
                        'total_judges': total_judges,
                        'total_scores': sum(m['total_scores'] for m in year_metrics)
                    })
        
        return pd.DataFrame(trends_data)
    
    def get_judge_consistency_metrics(self, judge_id, metric='throwout_rate', score_type='both'):
        """Calculate consistency metrics for a judge over time"""
        
        # Get temporal data for this judge
        trends_df = self.get_temporal_trends_data(judge_id, 'year', metric, score_type)
        
        if trends_df.empty or len(trends_df) < 2:
            return {
                'trend_direction': 'insufficient_data',
                'trend_strength': 0,
                'consistency_score': 0,
                'variance': 0,
                'coefficient_variation': 0,
                'slope': 0,
                'p_value': 1.0
            }
        
        # Calculate trend metrics
        values = trends_df['metric_value'].values
        time_periods = range(len(values))
        
        # Linear regression for trend
        try:
            slope, intercept, r_value, p_value, std_err = linregress(time_periods, values)
        except Exception as e:
            # Handle cases where linregress fails (e.g., all values are the same)
            slope, r_value, p_value = 0.0, 0.0, 1.0
        
        # Determine trend direction
        if abs(slope) < 0.1:
            trend_direction = 'stable'
        elif slope > 0:
            trend_direction = 'increasing'
        else:
            trend_direction = 'decreasing'
        
        # Calculate consistency metrics
        variance = np.var(values)
        mean_value = np.mean(values)
        coefficient_variation = (np.std(values) / mean_value * 100) if mean_value > 0 else 0
        
        # Consistency score (lower variance = higher consistency)
        max_possible_variance = (np.max(values) - np.min(values)) ** 2 / 4
        consistency_score = max(0, 100 - (variance / max_possible_variance * 100)) if max_possible_variance > 0 else 100
        
        return {
            'trend_direction': trend_direction,
            'trend_strength': abs(r_value),
            'consistency_score': round(consistency_score, 2),
            'variance': round(variance, 2),
            'coefficient_variation': round(coefficient_variation, 2),
            'slope': round(slope, 4),
            'p_value': round(p_value, 4)
        }
    
    def calculate_statistical_significance(self, judge_id, competition_ids=None, discipline_type_ids=None, year_filter=None):
        """Calculate statistical significance tests for judge bias detection"""
        from scipy import stats
        
        # Get judge data
        pcs_df = self.get_judge_pcs_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
        element_df = self.get_judge_element_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
        
        if pcs_df.empty and element_df.empty:
            return {
                'pcs_tests': {},
                'element_tests': {},
                'overall_significance': False,
                'bias_detected': False
            }
        
        results = {
            'pcs_tests': {},
            'element_tests': {},
            'overall_significance': False,
            'bias_detected': False
        }
        
        # PCS Statistical Tests
        if not pcs_df.empty:
            # Test 1: One-sample t-test for deviation from zero
            deviations = pcs_df['deviation'].values
            t_stat_pcs, p_val_pcs = stats.ttest_1samp(deviations, 0)
            
            # Test 2: Chi-square test for throwout rate
            throwouts = pcs_df['thrown_out'].sum()
            total_pcs = len(pcs_df)
            expected_throwout_rate = 0.05  # Expected 5% throwout rate
            expected_throwouts = total_pcs * expected_throwout_rate
            
            if expected_throwouts > 0:
                chi2_pcs, p_chi2_pcs = stats.chisquare([throwouts, total_pcs - throwouts], 
                                                      [expected_throwouts, total_pcs - expected_throwouts])
            else:
                chi2_pcs, p_chi2_pcs = 0, 1.0
            
            # Test 3: Normality test for deviations (Shapiro-Wilk)
            if len(deviations) >= 3:
                shapiro_stat_pcs, shapiro_p_pcs = stats.shapiro(deviations)
            else:
                shapiro_stat_pcs, shapiro_p_pcs = 1.0, 1.0
            
            # Test 4: Outlier detection using z-score
            if len(deviations) > 1:
                z_scores_pcs = np.abs(stats.zscore(deviations))
                outliers_pcs = np.sum(z_scores_pcs > 2.58)  # 99% confidence level
            else:
                outliers_pcs = 0
            outlier_rate_pcs = outliers_pcs / len(deviations) if len(deviations) > 0 else 0
            
            results['pcs_tests'] = {
                'deviation_ttest': {
                    'statistic': round(t_stat_pcs, 4),
                    'p_value': round(p_val_pcs, 4),
                    'significant': p_val_pcs < 0.05,
                    'interpretation': 'Systematic bias detected' if p_val_pcs < 0.05 else 'No systematic bias'
                },
                'throwout_chi2': {
                    'statistic': round(chi2_pcs, 4),
                    'p_value': round(p_chi2_pcs, 4),
                    'significant': p_chi2_pcs < 0.05,
                    'actual_rate': round(throwouts / total_pcs * 100, 2),
                    'expected_rate': 5.0,
                    'interpretation': 'Unusual throwout pattern' if p_chi2_pcs < 0.05 else 'Normal throwout pattern'
                },
                'normality_test': {
                    'statistic': round(shapiro_stat_pcs, 4),
                    'p_value': round(shapiro_p_pcs, 4),
                    'normal_distribution': shapiro_p_pcs > 0.05,
                    'interpretation': 'Normal scoring pattern' if shapiro_p_pcs > 0.05 else 'Non-normal scoring pattern'
                },
                'outlier_analysis': {
                    'outlier_count': int(outliers_pcs),
                    'outlier_rate': round(outlier_rate_pcs * 100, 2),
                    'excessive_outliers': outlier_rate_pcs > 0.05,
                    'interpretation': 'Excessive outliers detected' if outlier_rate_pcs > 0.05 else 'Normal outlier rate'
                }
            }
        
        # Element Statistical Tests
        if not element_df.empty:
            # Test 1: One-sample t-test for deviation from zero
            deviations = element_df['deviation'].values
            t_stat_elem, p_val_elem = stats.ttest_1samp(deviations, 0)
            
            # Test 2: Chi-square test for throwout rate
            throwouts = element_df['thrown_out'].sum()
            total_elem = len(element_df)
            expected_throwout_rate = 0.05  # Expected 5% throwout rate
            expected_throwouts = total_elem * expected_throwout_rate
            
            if expected_throwouts > 0:
                chi2_elem, p_chi2_elem = stats.chisquare([throwouts, total_elem - throwouts], 
                                                        [expected_throwouts, total_elem - expected_throwouts])
            else:
                chi2_elem, p_chi2_elem = 0, 1.0
            
            # Test 3: Normality test for deviations (Shapiro-Wilk)
            if len(deviations) >= 3:
                shapiro_stat_elem, shapiro_p_elem = stats.shapiro(deviations)
            else:
                shapiro_stat_elem, shapiro_p_elem = 1.0, 1.0
            
            # Test 4: Outlier detection using z-score
            if len(deviations) > 1:
                z_scores_elem = np.abs(stats.zscore(deviations))
                outliers_elem = np.sum(z_scores_elem > 2.58)  # 99% confidence level
            else:
                outliers_elem = 0
            outlier_rate_elem = outliers_elem / len(deviations) if len(deviations) > 0 else 0
            
            results['element_tests'] = {
                'deviation_ttest': {
                    'statistic': round(t_stat_elem, 4),
                    'p_value': round(p_val_elem, 4),
                    'significant': p_val_elem < 0.05,
                    'interpretation': 'Systematic bias detected' if p_val_elem < 0.05 else 'No systematic bias'
                },
                'throwout_chi2': {
                    'statistic': round(chi2_elem, 4),
                    'p_value': round(p_chi2_elem, 4),
                    'significant': p_chi2_elem < 0.05,
                    'actual_rate': round(throwouts / total_elem * 100, 2),
                    'expected_rate': 5.0,
                    'interpretation': 'Unusual throwout pattern' if p_chi2_elem < 0.05 else 'Normal throwout pattern'
                },
                'normality_test': {
                    'statistic': round(shapiro_stat_elem, 4),
                    'p_value': round(shapiro_p_elem, 4),
                    'normal_distribution': shapiro_p_elem > 0.05,
                    'interpretation': 'Normal scoring pattern' if shapiro_p_elem > 0.05 else 'Non-normal scoring pattern'
                },
                'outlier_analysis': {
                    'outlier_count': int(outliers_elem),
                    'outlier_rate': round(outlier_rate_elem * 100, 2),
                    'excessive_outliers': outlier_rate_elem > 0.05,
                    'interpretation': 'Excessive outliers detected' if outlier_rate_elem > 0.05 else 'Normal outlier rate'
                }
            }
        
        # Overall significance assessment
        significant_tests = 0
        total_tests = 0
        
        for test_category in [results['pcs_tests'], results['element_tests']]:
            if test_category:
                for test_name, test_result in test_category.items():
                    if 'significant' in test_result:
                        total_tests += 1
                        if test_result['significant']:
                            significant_tests += 1
                    elif 'excessive_outliers' in test_result:
                        total_tests += 1
                        if test_result['excessive_outliers']:
                            significant_tests += 1
        
        results['overall_significance'] = significant_tests > 0
        results['bias_detected'] = significant_tests >= 2  # Require at least 2 significant tests
        results['significance_ratio'] = round(significant_tests / total_tests, 2) if total_tests > 0 else 0
        
        return results
    
    def get_bias_detection_summary(self, competition_ids=None, discipline_type_ids=None, year_filter=None):
        """Get a summary of bias detection across all judges"""
        
        judges = self.session.query(Judge.id, Judge.name, Judge.location).all()
        bias_summary = []
        
        for judge_id, judge_name, location in judges:
            # Get statistical significance results
            significance_results = self.calculate_statistical_significance(
                judge_id, competition_ids, discipline_type_ids, year_filter
            )
            
            if significance_results['pcs_tests'] or significance_results['element_tests']:
                # Get basic stats
                pcs_df = self.get_judge_pcs_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
                element_df = self.get_judge_element_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
                stats_summary = self.calculate_judge_summary_stats(pcs_df, element_df)
                
                bias_summary.append({
                    'judge_id': judge_id,
                    'judge_name': judge_name,
                    'location': location or 'Unknown',
                    'bias_detected': significance_results['bias_detected'],
                    'overall_significance': significance_results['overall_significance'],
                    'significance_ratio': significance_results['significance_ratio'],
                    'total_scores': stats_summary['pcs_total_scores'] + stats_summary['element_total_scores'],
                    'pcs_throwout_rate': stats_summary['pcs_throwout_rate'],
                    'element_throwout_rate': stats_summary['element_throwout_rate'],
                    'pcs_anomaly_rate': stats_summary['pcs_anomaly_rate'],
                    'element_anomaly_rate': stats_summary['element_anomaly_rate']
                })
        
        return pd.DataFrame(bias_summary)
    
    def compare_judge_distributions(self, judge_id_1, judge_id_2, score_type='both'):
        """Compare two judges' scoring distributions using statistical tests"""
        from scipy import stats
        
        # Initialize dataframes
        pcs_df_1 = pd.DataFrame()
        pcs_df_2 = pd.DataFrame()
        element_df_1 = pd.DataFrame()
        element_df_2 = pd.DataFrame()
        
        # Get data for both judges
        if score_type in ['pcs', 'both']:
            pcs_df_1 = self.get_judge_pcs_stats(judge_id_1)
            pcs_df_2 = self.get_judge_pcs_stats(judge_id_2)
        
        if score_type in ['element', 'both']:
            element_df_1 = self.get_judge_element_stats(judge_id_1)
            element_df_2 = self.get_judge_element_stats(judge_id_2)
        
        comparison_results = {}
        
        # PCS comparison
        if score_type in ['pcs', 'both'] and not pcs_df_1.empty and not pcs_df_2.empty:
            deviations_1 = pcs_df_1['deviation'].values
            deviations_2 = pcs_df_2['deviation'].values
            
            # Mann-Whitney U test (non-parametric)
            u_stat, u_p = stats.mannwhitneyu(deviations_1, deviations_2, alternative='two-sided')
            
            # Kolmogorov-Smirnov test
            ks_stat, ks_p = stats.ks_2samp(deviations_1, deviations_2)
            
            # T-test for means
            t_stat, t_p = stats.ttest_ind(deviations_1, deviations_2)
            
            comparison_results['pcs'] = {
                'mannwhitney_u': {
                    'statistic': round(u_stat, 4),
                    'p_value': round(u_p, 4),
                    'significant': u_p < 0.05,
                    'interpretation': 'Different distributions' if u_p < 0.05 else 'Similar distributions'
                },
                'kolmogorov_smirnov': {
                    'statistic': round(ks_stat, 4),
                    'p_value': round(ks_p, 4),
                    'significant': ks_p < 0.05,
                    'interpretation': 'Different distributions' if ks_p < 0.05 else 'Similar distributions'
                },
                'ttest': {
                    'statistic': round(t_stat, 4),
                    'p_value': round(t_p, 4),
                    'significant': t_p < 0.05,
                    'interpretation': 'Different means' if t_p < 0.05 else 'Similar means'
                }
            }
        
        # Element comparison
        if score_type in ['element', 'both'] and not element_df_1.empty and not element_df_2.empty:
            deviations_1 = element_df_1['deviation'].values
            deviations_2 = element_df_2['deviation'].values
            
            # Mann-Whitney U test (non-parametric)
            u_stat, u_p = stats.mannwhitneyu(deviations_1, deviations_2, alternative='two-sided')
            
            # Kolmogorov-Smirnov test
            ks_stat, ks_p = stats.ks_2samp(deviations_1, deviations_2)
            
            # T-test for means
            t_stat, t_p = stats.ttest_ind(deviations_1, deviations_2)
            
            comparison_results['element'] = {
                'mannwhitney_u': {
                    'statistic': round(u_stat, 4),
                    'p_value': round(u_p, 4),
                    'significant': u_p < 0.05,
                    'interpretation': 'Different distributions' if u_p < 0.05 else 'Similar distributions'
                },
                'kolmogorov_smirnov': {
                    'statistic': round(ks_stat, 4),
                    'p_value': round(ks_p, 4),
                    'significant': ks_p < 0.05,
                    'interpretation': 'Different distributions' if ks_p < 0.05 else 'Similar distributions'
                },
                'ttest': {
                    'statistic': round(t_stat, 4),
                    'p_value': round(t_p, 4),
                    'significant': t_p < 0.05,
                    'interpretation': 'Different means' if t_p < 0.05 else 'Similar means'
                }
            }
        
        return comparison_results
