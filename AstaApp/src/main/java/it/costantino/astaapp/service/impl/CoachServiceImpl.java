package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Coach;
import it.costantino.astaapp.repository.CoachRepository;
import it.costantino.astaapp.service.CoachService;
import it.costantino.astaapp.service.dto.CoachDTO;
import it.costantino.astaapp.service.mapper.CoachMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Coach}.
 */
@Service
@Transactional
public class CoachServiceImpl implements CoachService {

    private static final Logger LOG = LoggerFactory.getLogger(CoachServiceImpl.class);

    private final CoachRepository coachRepository;

    private final CoachMapper coachMapper;

    public CoachServiceImpl(CoachRepository coachRepository, CoachMapper coachMapper) {
        this.coachRepository = coachRepository;
        this.coachMapper = coachMapper;
    }

    @Override
    public CoachDTO save(CoachDTO coachDTO) {
        LOG.debug("Request to save Coach : {}", coachDTO);
        Coach coach = coachMapper.toEntity(coachDTO);
        coach = coachRepository.save(coach);
        return coachMapper.toDto(coach);
    }

    @Override
    public CoachDTO update(CoachDTO coachDTO) {
        LOG.debug("Request to update Coach : {}", coachDTO);
        Coach coach = coachMapper.toEntity(coachDTO);
        coach = coachRepository.save(coach);
        return coachMapper.toDto(coach);
    }

    @Override
    public Optional<CoachDTO> partialUpdate(CoachDTO coachDTO) {
        LOG.debug("Request to partially update Coach : {}", coachDTO);

        return coachRepository
            .findById(coachDTO.getId())
            .map(existingCoach -> {
                coachMapper.partialUpdate(existingCoach, coachDTO);

                return existingCoach;
            })
            .map(coachRepository::save)
            .map(coachMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<CoachDTO> findAll() {
        LOG.debug("Request to get all Coaches");
        return coachRepository.findAll().stream().map(coachMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<CoachDTO> findOne(Long id) {
        LOG.debug("Request to get Coach : {}", id);
        return coachRepository.findById(id).map(coachMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Coach : {}", id);
        coachRepository.deleteById(id);
    }
}
